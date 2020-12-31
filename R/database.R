#' Methods for working with Cosmos DB databases
#'
#' @param endpoint A Cosmos DB endpoint object as obtained from `cosmos_endpoint`, or for `delete_cosmos_database.cosmos_database`, the database object.
#' @param name The name of the Cosmos DB database.
#' @param autoscale_maxRUs,manual_RUs For `create_cosmos_database`, optional parameters for the maximum request units (RUs) allowed. See the Cosmos DB documentation for more details.
#' @param headers For `create_cosmos_database`, optional HTTP headers to include in the request.
#' @param confirm For `delete_cosmos_database`, whether to ask for confirmation before deleting.
#' @param ... Arguments passed to lower-level functions.
#' @details
#' These are methods for managing Cosmos DB databases using the core (SQL) API.
#' @aliases cosmos_database
#' @rdname cosmos_database
#' @export
get_cosmos_database <- function(endpoint, ...)
{
    UseMethod("get_cosmos_database")
}

#' @rdname cosmos_database
#' @export
get_cosmos_database.cosmos_endpoint <- function(endpoint, name, ...)
{
    path <- file.path("dbs", name)
    res <- do_cosmos_op(endpoint, path, "dbs", path, ...)
    obj <- process_cosmos_response(res)
    class(obj) <- "cosmos_database"
    obj$endpoint <- endpoint
    obj
}


#' @rdname cosmos_database
#' @export
create_cosmos_database <- function(endpoint, ...)
{
    UseMethod("create_cosmos_database")
}

#' @rdname cosmos_database
#' @export
create_cosmos_database.cosmos_endpoint <- function(endpoint, name, autoscale_maxRUs=NULL, manual_RUs=NULL, headers=list(), ...)
{
    if(!is.null(manual_RUs))
        headers$`x-ms-offer-throughput` <- manual_RUs
    if(!is.null(autoscale_maxRUs))
        headers$`x-ms-cosmos-offer-autopilot-settings` <- jsonlite::toJSON(autoscale_maxRUs)

    body <- list(id=name)
    res <- do_cosmos_op(endpoint, "dbs", "dbs", "", headers=headers, body=body, encode="json", http_verb="POST", ...)
    obj <- process_cosmos_response(res)
    class(obj) <- "cosmos_database"
    obj$endpoint <- endpoint
    obj
}


#' @rdname cosmos_database
#' @export
delete_cosmos_database <- function(endpoint, ...)
{
    UseMethod("delete_cosmos_database")
}

#' @rdname cosmos_database
#' @export
delete_cosmos_database.cosmos_endpoint <- function(endpoint, name, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, name, "database"))
        return(invisible(NULL))

    path <- file.path("dbs", name)
    res <- do_cosmos_op(endpoint, path, "dbs", path, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res))
}

#' @rdname cosmos_database
#' @export
delete_cosmos_database.cosmos_database <- function(endpoint, ...)
{
    delete_cosmos_database(endpoint$endpoint, endpoint$id, ...)
}


#' @rdname cosmos_database
#' @export
list_cosmos_databases <- function(endpoint, ...)
{
    UseMethod("list_cosmos_databases")
}

#' @rdname cosmos_database
#' @export
list_cosmos_databases.cosmos_endpoint <- function(endpoint, ...)
{
    res <- do_cosmos_op(endpoint, "dbs", "dbs", "", ...)
    AzureRMR::named_list(lapply(process_cosmos_response(res)$Databases, function(obj)
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

