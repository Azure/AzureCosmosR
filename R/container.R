#' Methods for working with Azure Cosmos DB containers
#'
#' @param object A Cosmos DB database object, as obtained from `get_cosmos_database` or `create_cosmos_database`, or for `delete_cosmos_container.cosmos_container`, the container object.
#' @param database For `get_cosmos_container.cosmos_endpoint`, the name of the database that includes the container.
#' @param container The name of the container.
#' @param partition_key For `create_cosmos_container`, the name of the partition key.
#' @param partition_version For `create_cosmos_container`, the partition version. Can be either 1 or 2. Version 2 supports large partition key values (longer than 100 bytes) but requires API version `2018-12-31` or later. Use version 1 if the container needs to be accessible to older Cosmos DB SDKs.
#' @param autoscale_maxRUs,manual_RUs For `create_cosmos_container`, optional parameters for the maximum request units (RUs) allowed. See the Cosmos DB documentation for more details.
#' @param confirm For `delete_cosmos_container`, whether to ask for confirmation before deleting.
#' @param headers,... Optional arguments passed to lower-level functions.
#' @details
#' These are methods for working with Cosmos DB containers using the core (SQL) API. A container is analogous to a table in SQL, or a collection in MongoDB.
#'
#' `get_cosmos_container`, `create_cosmos_container`, `delete_cosmos_container` and `list_cosmos_containers` provide basic container management functionality.
#'
#' `get_partition_key` returns the name of the partition key column in the container, and `list_partition_key_values` returns all the distinct values for this column. These are useful when working with queries that have to be mapped across partitions.
#' @return
#' For `get_cosmos_container` and `create_cosmos_container`, an object of class `cosmos_container. For `list_cosmos_container`, a list of such objects.
#' @seealso
#' [cosmos_container], [query_documents], [bulk_import], [bulk_delete]
#' @examples
#' \dontrun{
#'
#' endp <- cosmos_endpoint("https://myaccount.documents.azure.com:443/", key="mykey")
#' db <- get_cosmos_database(endp, "mydatabase")
#'
#' create_cosmos_container(db, "mycontainer", partition_key="sex")
#'
#' list_cosmos_containers(db)
#'
#' cont <- get_cosmos_container(db, "mycontainer")
#'
#' delete_cosmos_container(cont)
#'
#' }
#' @aliases cosmos_container
#' @rdname cosmos_container
#' @export
get_cosmos_container <- function(object, ...)
{
    UseMethod("get_cosmos_container")
}

#' @rdname cosmos_container
#' @export
get_cosmos_container.cosmos_database <- function(object, container, ...)
{
    path <- file.path("colls", container)
    res <- do_cosmos_op(object, path, "colls", path, ...)
    obj <- process_cosmos_response(res)
    obj$database <- object
    class(obj) <- "cosmos_container"
    obj
}

#' @rdname cosmos_container
#' @export
get_cosmos_container.cosmos_endpoint <- function(object, database, container, ...)
{
    path <- file.path("dbs", database, "colls", container)
    res <- do_cosmos_op(object, path, "colls", path, ...)
    obj <- process_cosmos_response(res)
    obj$database <- structure(list(endpoint=object, id=database), class="cosmos_database")
    class(obj) <- "cosmos_container"
    obj
}


#' @rdname cosmos_container
#' @export
create_cosmos_container <- function(object, ...)
{
    UseMethod("create_cosmos_container")
}

#' @rdname cosmos_container
#' @export
create_cosmos_container.cosmos_database <- function(object, container, partition_key, partition_version=2,
    autoscale_maxRUs=NULL, manual_RUs=NULL, headers=list(), ...)
{
    if(!is.null(manual_RUs))
        headers$`x-ms-offer-throughput` <- manual_RUs
    if(!is.null(autoscale_maxRUs))
        headers$`x-ms-cosmos-offer-autopilot-settings` <- jsonlite::toJSON(autoscale_maxRUs)

    body <- list(
        id=container,
        partitionKey=list(
            paths=list(paste0("/", partition_key)),
            kind="Hash",
            version=partition_version
        )
    )

    res <- do_cosmos_op(object, "colls", "colls", "", headers=headers, body=body, encode="json",
                        http_verb="POST", ...)
    obj <- process_cosmos_response(res)
    obj$database <- object
    class(obj) <- "cosmos_container"
    obj

}


#' @rdname cosmos_container
#' @export
delete_cosmos_container <- function(object, ...)
{
    UseMethod("delete_cosmos_container")
}

#' @rdname cosmos_container
#' @export
delete_cosmos_container.cosmos_database <- function(object, container, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, container, "container"))
        return(invisible(NULL))

    path <- file.path("colls", container)
    res <- do_cosmos_op(object, path, "colls", path, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res))
}

#' @rdname cosmos_container
#' @export
delete_cosmos_container.cosmos_container <- function(object, ...)
{
    delete_cosmos_container(object$database, object$id, ...)
}


#' @rdname cosmos_container
#' @export
list_cosmos_containers <- function(object, ...)
{
    UseMethod("list_cosmos_containers")
}

#' @rdname cosmos_container
#' @export
list_cosmos_containers.cosmos_database <- function(object, ...)
{
    res <- do_cosmos_op(object, "colls", "colls", "", ...)
    AzureRMR::named_list(lapply(process_cosmos_response(res)$DocumentCollections, function(obj)
    {
        obj$database <- object
        structure(obj, class="cosmos_container")
    }), "id")
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
