#' @export
get_database <- function(endpoint, name)
{
    path <- file.path("dbs", name)
    res <- call_cosmos_endpoint(endpoint, path, "dbs", path)
    obj <- process_cosmos_response(res)
    class(obj) <- "cosmos_database"
    obj$endpoint <- endpoint
    obj
}

#' @export
print.cosmos_database <- function(x, ...)
{
    cat("Cosmos DB SQL database '", x$id, "'\n", sep="")
    path <- x$endpoint$host
    path$path <- x$id
    cat("Path:", httr::build_url(path))
    invisible(x)
}

#' @export
do_cosmos_op <- function(object, ...)
{
    UseMethod("do_cosmos_op")
}

#' @export
do_cosmos_op.cosmos_database <- function(object, path="", resource_type="dbs", resource_link, ...)
{
    if(missing(resource_link))
        resource_link <- file.path("dbs", object$database$id, "colls", object$id)
    path <- if(nchar(path) > 0)
        file.path("dbs", object$id, path)
    else file.path("dbs", object$id)
    call_cosmos_endpoint(object$endpoint, path, resource_type, resource_link, ...)
}

