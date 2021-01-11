#' Carry out a Cosmos DB operation
#'
#' @param object A Cosmos DB endpoint, database, container or document object.
#' @param path The (partial) URL path for the operation.
#' @param resource_type The type of resource. For most purposes, the default value should suffice.
#' @param resource_link The resource link for authorization. For most purposes, the default value should suffice.
#' @param headers Any optional HTTP headers to include in the API call.
#' @param ... Arguments passed to lower-level functions.
#' @details
#' `do_cosmos_op` provides a higher-level interface to the Cosmos DB REST API than `call_cosmos_endpoint`. In particular, it sets the `resource_type` and `resource_link` arguments to sensible defaults, and fills in the beginning of the URL path for the REST call.
#' @return
#' The result of `call_cosmos_endpoint`: either a httr response object, or a list of such objects. Call `process_cosmos_response` to extract the result of the call.
#' @examples
#' \dontrun{
#'
#' endp <- cosmos_endpoint("https://myaccount.documents.azure.com:443/", key="mykey")
#'
#' db <- get_cosmos_database(endp, "mydatabase")
#' do_cosmos_op(db) %>%
#'     process_cosmos_response()
#'
#' cont <- get_cosmos_container(db, "mycontainer")
#' do_cosmos_op(cont) %>%
#'     process_cosmos_response()
#'
#' }
#' @rdname do_cosmos_op
#' @export
do_cosmos_op <- function(object, ...)
{
    UseMethod("do_cosmos_op")
}

#' @rdname do_cosmos_op
#' @export
do_cosmos_op.cosmos_endpoint <- function(object, ...)
{
    call_cosmos_endpoint(object, ...)
}

#' @rdname do_cosmos_op
#' @export
do_cosmos_op.cosmos_database <- function(object, path="", resource_type="dbs", resource_link="", ...)
{
    full_path <- full_reslink <-  file.path("dbs", object$id)
    if(nchar(path) > 0)
        full_path <- file.path(full_path, path)
    if(nchar(resource_link) > 0)
        full_reslink <- file.path(full_reslink, resource_link)
    call_cosmos_endpoint(object$endpoint, full_path, resource_type, full_reslink, ...)
}

#' @rdname do_cosmos_op
#' @export
do_cosmos_op.cosmos_container <- function(object, path="", resource_type="colls", resource_link="", ...)
{
    full_path <- full_reslink <-  file.path("dbs", object$database$id, "colls", object$id)
    if(nchar(path) > 0)
        full_path <- file.path(full_path, path)
    if(nchar(resource_link) > 0)
        full_reslink <- file.path(full_reslink, resource_link)
    call_cosmos_endpoint(object$database$endpoint, full_path, resource_type, full_reslink, ...)
}

#' @rdname do_cosmos_op
#' @export
do_cosmos_op.cosmos_document <- function(object, path="", resource_type="docs", resource_link="", headers=list(), ...)
{
    full_path <- full_reslink <-  file.path("dbs", object$container$database$id,
        "colls", object$container$id, "docs", object$data$id)
    if(nchar(path) > 0)
        full_path <- file.path(full_path, path)
    if(nchar(resource_link) > 0)
        full_reslink <- file.path(full_reslink, resource_link)

    partition_key <- sub("^/", "", object$container$partitionKey$paths)
    headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(object$data[[partition_key]])
    call_cosmos_endpoint(object$container$database$endpoint, full_path, resource_type, full_reslink,
                         headers=headers, ...)
}


