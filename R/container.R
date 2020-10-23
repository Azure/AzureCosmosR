#' @export
get_container <- function(database, name)
{
    res <- do_database_op(database, file.path("colls", name), "colls")
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

