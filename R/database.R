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
    cat("Cosmos DB SQL database\n")
    path <- x$endpoint$host
    path$path <- x$id
    cat("Path:", httr::build_url(path))
    invisible(x)
}

