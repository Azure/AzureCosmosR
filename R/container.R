#' @export
get_container <- function(database, name)
{
    res <- do_cosmos_op(database, file.path("colls", name), "colls")
    obj <- process_cosmos_response(res)
    class(obj) <- "cosmos_container"
    obj$database <- database
    obj
}

#' @export
print.cosmos_container <- function(x, ...)
{
    cat("Cosmos DB SQL container '", x$id, "'\n", sep="")
    path <- x$database$endpoint$host
    path$path <- file.path("dbs", x$database$id, "colls", x$id)
    cat("Path:", httr::build_url(path), "\n")
    invisible(x)
}

#' @export
do_cosmos_op.cosmos_container <- function(object, path="", resource_type, resource_link, ...)
{
    if(missing(resource_link))
        resource_link <- file.path("dbs", object$database$id, "colls", object$id)
    path <- if(nchar(path) > 0)
        file.path("colls", object$id, path)
    else file.path("colls", object$id)
    do_cosmos_op(object$database, path, resource_type, resource_link, ...)
}

