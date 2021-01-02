#' Methods for working with Azure Cosmos DB databases
#'
#' @param object A Cosmos DB endpoint object as obtained from `cosmos_endpoint`, or for `delete_cosmos_database.cosmos_database`, the database object.
#' @param database The name of the Cosmos DB database.
#' @param autoscale_maxRUs,manual_RUs For `create_cosmos_database`, optional parameters for the maximum request units (RUs) allowed. See the Cosmos DB documentation for more details.
#' @param confirm For `delete_cosmos_database`, whether to ask for confirmation before deleting.
#' @param headers,... Optional arguments passed to lower-level functions.
#' @details
#' These are methods for managing Cosmos DB databases using the core (SQL) API.
#' @return
#' `get_cosmos_database` and `create_cosmos_database` return an object of class `cosmos_database`. `list_cosmos_databases` returns a list of such objects.
#' @examples
#' \dontrun{
#'
#' endp <- cosmos_endpoint("https://myaccount.documents.azure.com:443/", key="mykey")
#'
#' create_cosmos_database(endp, "mydatabase")
#'
#' list_cosmos_databases(endp)
#'
#' db <- get_cosmos_database(endp, "mydatabase")
#'
#' delete_cosmos_database(db)
#'
#' }
#' @aliases cosmos_database
#' @rdname cosmos_database
#' @export
get_cosmos_database <- function(object, ...)
{
    UseMethod("get_cosmos_database")
}

#' @rdname cosmos_database
#' @export
get_cosmos_database.cosmos_endpoint <- function(object, database, ...)
{
    path <- file.path("dbs", database)
    res <- do_cosmos_op(object, path, "dbs", path, ...)
    obj <- process_cosmos_response(res)
    class(obj) <- "cosmos_database"
    obj$endpoint <- object
    obj
}


#' @rdname cosmos_database
#' @export
create_cosmos_database <- function(object, ...)
{
    UseMethod("create_cosmos_database")
}

#' @rdname cosmos_database
#' @export
create_cosmos_database.cosmos_endpoint <- function(object, database, autoscale_maxRUs=NULL, manual_RUs=NULL,
    headers=list(), ...)
{
    if(!is.null(manual_RUs))
        headers$`x-ms-offer-throughput` <- manual_RUs
    if(!is.null(autoscale_maxRUs))
        headers$`x-ms-cosmos-offer-autopilot-settings` <- jsonlite::toJSON(autoscale_maxRUs)

    body <- list(id=database)
    res <- do_cosmos_op(object, "dbs", "dbs", "", headers=headers, body=body, encode="json", http_verb="POST", ...)
    obj <- process_cosmos_response(res)
    class(obj) <- "cosmos_database"
    obj$endpoint <- object
    obj
}


#' @rdname cosmos_database
#' @export
delete_cosmos_database <- function(object, ...)
{
    UseMethod("delete_cosmos_database")
}

#' @rdname cosmos_database
#' @export
delete_cosmos_database.cosmos_endpoint <- function(object, database, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, database, "database"))
        return(invisible(NULL))

    path <- file.path("dbs", database)
    res <- do_cosmos_op(object, path, "dbs", path, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res))
}

#' @rdname cosmos_database
#' @export
delete_cosmos_database.cosmos_database <- function(object, ...)
{
    delete_cosmos_database(object$endpoint, object$id, ...)
}


#' @rdname cosmos_database
#' @export
list_cosmos_databases <- function(object, ...)
{
    UseMethod("list_cosmos_databases")
}

#' @rdname cosmos_database
#' @export
list_cosmos_databases.cosmos_endpoint <- function(object, ...)
{
    res <- do_cosmos_op(object, "dbs", "dbs", "", ...)
    AzureRMR::named_list(lapply(process_cosmos_response(res)$Databases, function(obj)
    {
        obj$endpoint <- object
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

