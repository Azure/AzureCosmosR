#' @export
bulk_import <- function(container, ...)
{
    UseMethod("bulk_import")
}

#' @export
bulk_import.cosmos_container <- local({
    .sproc_created <- FALSE

    function(container, data, procname="_AzureCosmosR_bulkImport", init_chunksize=10000, ...)
    {
        # create the stored procedure if necessary
        if(!.sproc_created)
        {
            res <- tryCatch(create_stored_procedure(container, procname,
                readLines(system.file("srcjs/bulkUpload.js", package="AzureCosmosR"))), error=function(e) e)
            if(inherits(res, "error"))
                if(!(is.character(res$message) && grepl("HTTP 409", res$message)))  # proc already existing is ok
                    stop(res)
            .sproc_created <<- TRUE
        }

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
})

import_by_key <- function(container, key, data, procname, init_chunksize, headers=list(), ...)
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
        message("Rows imported: ", rows_imported, "  this chunk: ", length(this_chunk),
                "  average chunksize: ", avg_chunksize)

        # adjust chunksize based on observed import performance per chunk
        this_chunksize <- if(this_import < this_chunksize)
            (this_chunksize + this_import)/2
        else floor(avg_chunksize + this_chunksize*2/n)
    }
    rows_imported
}


#' @export
bulk_delete <- function(container, ...)
{
    UseMethod("bulk_delete")
}

#' @export
bulk_delete.cosmos_container <- local({
    .sproc_created <- FALSE

    function(container, query, ..., procname="_AzureCosmosR_bulkDelete")
    {
        # create the stored procedure if necessary
        if(!.sproc_created)
        {
            create_stored_procedure(container, procname,
                readLines(system.file("srcjs/bulkDelete.js", package="AzureCosmosR")))
            .sproc_created <<- TRUE
        }

        res <- call_stored_procedure.cosmos_container(container, procname, query, ...)
        count <- process_cosmos_response(res)
    }
})
