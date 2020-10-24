#' @export
get_database <- function(endpoint, ...)
{
    UseMethod("get_database")
}

#' @export
get_database.cosmos_endpoint <- function(endpoint, name, ...)
{
    path <- file.path("dbs", name)
    res <- do_cosmos_op(endpoint, path, "dbs", path, ...)
    obj <- process_cosmos_response(res, ...)
    class(obj) <- "cosmos_database"
    obj$endpoint <- endpoint
    obj
}


#' @export
create_database <- function(endpoint, ...)
{
    UseMethod("create_database")
}

#' @export
create_database.cosmos_endpoint <- function(endpoint, name, autoscale_maxRUs=NULL, manual_RUs=NULL, headers=list(), ...)
{
    if(!is.null(manual_RUs))
        headers$`x-ms-offer-throughput` <- manual_RUs
    if(!is.null(autoscale_maxRUs))
        headers$`x-ms-cosmos-offer-autopilot-settings` <- jsonlite::toJSON(autoscale_maxRUs)

    body <- list(id=name)
    res <- do_cosmos_op(endpoint, "dbs", "dbs", "", headers=headers, body=body, encode="json", http_verb="POST", ...)
    obj <- process_cosmos_response(res, ...)
    class(obj) <- "cosmos_database"
    obj$endpoint <- endpoint
    obj
}


#' @export
delete_database <- function(endpoint, ...)
{
    UseMethod("delete_database")
}

#' @export
delete_database.cosmos_endpoint <- function(endpoint, name, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, name, "database"))
        return(invisible(NULL))

    path <- file.path("dbs", name)
    res <- do_cosmos_op(endpoint, path, "dbs", path, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res, ...))
}

#' @export
delete_database.cosmos_database <- function(endpoint, ...)
{
    delete_database(endpoint$endpoint, endpoint$id, ...)
}


#' @export
list_databases <- function(endpoint, ...)
{
    UseMethod("list_databases")
}

#' @export
list_databases.cosmos_endpoint <- function(endpoint, ...)
{
    res <- do_cosmos_op(endpoint, "dbs", "dbs", "", ...)
    AzureRMR::named_list(lapply(process_cosmos_response(res, simplify=FALSE, ...)$Databases, function(obj)
    {
        obj$endpoint <- endpoint
        structure(obj, class="cosmos_database")
    }), "id")
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
do_cosmos_op.cosmos_database <- function(object, path="", resource_type="dbs", resource_link="", ...)
{
    path <- if(nchar(path) > 0)
        file.path("dbs", object$id, path)
    else file.path("dbs", object$id)
    resource_link <- if(nchar(resource_link) > 0)
        file.path("dbs", object$id, resource_link)
    else file.path("dbs", object$id)
    do_cosmos_op(object$endpoint, path, resource_type, resource_link, ...)
}

