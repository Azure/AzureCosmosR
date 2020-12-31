#' Carry out a Cosmos DB operation
#'
#' @param object A Cosmos DB endpoint, database, container or document object.
#' @param path The (partial) URL path for the operation.
#' @param resource_type The type of resource. For most purposes, the default value should suffice.
#' @param resource_link The resource link for authorization. For most purposes, the default value should suffice.
#' @param headers Any optional HTTP headers to include in the API call.
#' @param ... Arguments passed to lower-level functions.
#' @details
#' `do_cosmos_op` provides a higher-level interface to the Cosmos DB REST API than `call_cosmos_endpoint`. In particular, it sets the `resource_type` and `resource_link` arguments to sensible defaults, and fills in the beginning of the path for the REST call.
#' @return
#' The result of `call_cosmos_endpoint`: either a httr response object, or a list of such objects. Call `process_cosmos_response` to extract the result of the call.
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


