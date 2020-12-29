#' @export
bulk_import <- function(container, ...)
{
    UseMethod("bulk_import")
}

#' @export
bulk_import.cosmos_container <- function(container, data, init_chunksize=1000, verbose=TRUE,
    procname="_AzureCosmosR_bulkImport", ...)
{
    # create the stored procedure if necessary
    res <- tryCatch(create_stored_procedure(container, procname,
        readLines(system.file("srcjs/bulkUpload.js", package="AzureCosmosR"))), error=function(e) e)
    if(inherits(res, "error"))
        if(!(is.character(res$message) && grepl("HTTP 409", res$message)))  # proc already existing is ok
            stop(res)

    key <- get_partition_key(container)
    res <- if(is.null(key))
        import_by_key(container, NULL, data, procname, init_chunksize, ...)
    else
    {
        if(is.null(data[[key]]))
            stop("Data does not contain partition key", call.=FALSE)
        lapply(split(data, data[[key]]), function(partdata)
            import_by_key(container, partdata[[key]][1], partdata, procname, init_chunksize, ...))
    }
    invisible(res)
}

import_by_key <- function(container, key, data, procname, init_chunksize, headers=list(), verbose=TRUE, ...)
{
    rows_imported <- 0
    this_import <- 0
    this_chunksize <- avg_chunksize <- init_chunksize
    nrows <- nrow(data)
    n <- 0
    if(!is.null(key))
        headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(key)
    while(rows_imported < nrows)
    {
        n <- n + 1
        this_chunk <- seq(rows_imported + 1, min(nrows, rows_imported + this_chunksize))
        this_import <- call_stored_procedure(container, procname, list(data[this_chunk, ]), headers=headers, ...)
        rows_imported <- rows_imported + this_import
        avg_chunksize <- (avg_chunksize * (n-1))/n + this_chunksize/n
        if(verbose)
            message("Rows imported: ", rows_imported, "  this chunk: ", length(this_chunk),
                    "  average chunksize: ", avg_chunksize)

        # adjust chunksize based on observed import performance per chunk
        this_chunksize <- if(this_import < this_chunksize)
            (this_chunksize + this_import)/2
        else this_chunksize*2
    }
    rows_imported
}


#' @export
bulk_delete <- function(container, ...)
{
    UseMethod("bulk_delete")
}

#' @export
bulk_delete.cosmos_container <- function(container, query, partition_key,
    procname="_AzureCosmosR_bulkDelete", headers=list(), ...)
{
    # create the stored procedure if necessary
    res <- tryCatch(create_stored_procedure(container, procname,
        readLines(system.file("srcjs/bulkDelete.js", package="AzureCosmosR"))), error=function(e) e)
    if(inherits(res, "error"))
        if(!(is.character(res$message) && grepl("HTTP 409", res$message)))  # proc already existing is ok
            stop(res)

    if(!is.null(partition_key))
        headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(partition_key)

    if(length(query) > 1)
        query <- paste0(query, collapse="\n")
    deleted <- 0
    repeat
    {
        res <- call_stored_procedure.cosmos_container(container, procname, list(query), headers=headers, ...)
        deleted <- deleted + res$deleted
        if(!res$continuation)
            break
    }
    invisible(deleted)
}
