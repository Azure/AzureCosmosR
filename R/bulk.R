#' Import a set of documents to an Azure Cosmos DB container
#'
#' @param container A Cosmos DB container object, as obtained by `get_cosmos_container` or `create_cosmos_container`.
#' @param data The data to import. Can be a data frame, or a string containing JSON text.
#' @param init_chunksize The number of rows to import per chunk. `bulk_import` can adjust this number dynamically based on observed performance.
#' @param verbose Whether to print updates to the console as the import progresses.
#' @param procname The stored procedure name to use for the server-side import code. Change this if, for some reason, the default name is taken.
#' @param ... Optional arguments passed to lower-level functions.
#' @details
#' This is a convenience function to import a dataset into a container. It works by creating a stored procedure and then calling it in a loop, passing the to-be-imported data in chunks. The dataset must include a column for the container's partition key or an error will result.
#'
#' Note that this function is not meant for production use. In particular, if the import fails midway through, it will not clean up after itself: you should call `bulk_delete` to remove the remnants of a failed import.
#' @return
#' A list containing the number of rows imported, for each value of the partition key.
#' @seealso
#' [bulk_delete], [cosmos_container]
#' @examples
#' \dontrun{
#'
#' endp <- cosmos_endpoint("https://myaccount.documents.azure.com:443/", key="mykey")
#' db <- get_cosmos_database(endp, "mydatabase")
#' cont <- create_cosmos_container(db, "mycontainer", partition_key="sex")
#'
#' # importing the Star Wars data from dplyr
#' # notice that rows with sex=NA are not imported
#' bulk_import(cont, dplyr::starwars)
#'
#' # importing from a JSON file
#' writeLines(jsonlite::toJSON(dplyr::starwars), "starwars.json")
#' bulk_import(cont, "starwars.json")
#'
#' }
#' @rdname bulk_import
#' @export
bulk_import <- function(container, ...)
{
    UseMethod("bulk_import")
}

#' @rdname bulk_import
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

    if(is.character(data) && jsonlite::validate(data))
        data <- jsonlite::fromJSON(data, simplifyDataFrame=FALSE)

    key <- get_partition_key(container)
    res <- if(is.null(key))
        import_by_key(container, NULL, data, procname, init_chunksize, ...)
    else
    {
        if(is.null(data[[key]]))
            stop("Data does not contain partition key", call.=FALSE)
        lapply(split(data, data[[key]]), function(partdata)
            import_by_key(container, partdata[[key]][1], partdata, procname, init_chunksize, verbose=verbose, ...))
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
        this_import <- exec_stored_procedure(container, procname, list(data[this_chunk, ]), headers=headers, ...)
        rows_imported <- rows_imported + this_import
        avg_chunksize <- (avg_chunksize * (n-1))/n + this_chunksize/n
        if(verbose)
            message("Rows imported: ", rows_imported, "  this chunk: ", length(this_chunk),
                    "  average chunksize: ", avg_chunksize)

        # adjust chunksize based on observed import performance per chunk
        this_chunksize <- if(this_import < this_chunksize)
            (this_chunksize + this_import)/2
        else init_chunksize
    }
    rows_imported
}


#' Delete a set of documents from an Azure Cosmos DB container
#'
#' @param container A Cosmos DB container object, as obtained by `get_cosmos_container` or `create_cosmos_container`.
#' @param query A query specifying which documents to delete.
#' @param partition_key Optionally, limit the deletion only to documents with this key value.
#' @param procname The stored procedure name to use for the server-side import code. Change this if, for some reason, the default name is taken.
#' @param headers,... Optional arguments passed to lower-level functions.
#' @details
#' This is a convenience function to delete multiple documents from a container. It works by creating a stored procedure and then calling it with the supplied query as a parameter. This function is not meant for production use.
#' @return
#' The number of rows deleted.
#' @seealso
#' [bulk_import], [cosmos_container]
#' @examples
#' \dontrun{
#'
#' endp <- cosmos_endpoint("https://myaccount.documents.azure.com:443/", key="mykey")
#' db <- get_cosmos_database(endp, "mydatabase")
#' cont <- create_cosmos_container(db, "mycontainer", partition_key="sex")
#'
#'  # importing the Star Wars data from dplyr
#' bulk_import(cont, dplyr::starwars)
#'
#' # deleting a subset of documents
#' bulk_delete(cont, "select * from mycontainer c where c.gender = 'masculine'")
#'
#' # deleting documents for a specific partition key value
#' bulk_delete(cont, "select * from mycontainer", partition_key="male")
#'
#' # deleting all documents
#' bulk_delete(cont, "select * from mycontainer")
#'
#' }
#' @rdname bulk_delete
#' @export
bulk_delete <- function(container, ...)
{
    UseMethod("bulk_delete")
}

#' @rdname bulk_delete
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
        res <- exec_stored_procedure.cosmos_container(container, procname, list(query), headers=headers, ...)
        deleted <- deleted + res$deleted
        if(!res$continuation)
            break
    }
    invisible(deleted)
}
