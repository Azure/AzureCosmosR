#' @export
get_database <- function(endpoint, name)
{
    res <- call_cosmos_endpoint(endpoint, file.path("dbs", name), "dbs")
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
do_database_op <- function(database, ...)
{
    UseMethod("do_database_op")
}

#' @export
do_database_op.cosmos_database <- function(database, operation="", ...)
{
    operation <- if(nchar(operation) > 0)
        file.path("dbs", database$id, operation)
    else file.path("dbs", database$id)
    call_cosmos_endpoint(database$endpoint, operation, ...)
}

