#' Methods for working with Azure Cosmos DB containers
#'
#' @param database A Cosmos DB database object, as obtained from `get_cosmos_database` or `create_cosmos_database`, or for `delete_cosmos_container.cosmos_container`, the container object.
#' @param name,container The name of the container.
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
#' `get_partition_key` returns the name of the partition key column in the container, and `list_key_values` returns all the distinct values for this column. These are useful when working with queries that have to be mapped across partitions.
#' @return
#' For `get_cosmos_container` and `create_cosmos_container`, an object of class `cosmos_container. For `list_cosmos_container`, a list of such objects.
#'
#' For `get_partition_key`, the name of the partition key column as a string. For `list_key_values`, a character vector of all the values of the partition key.
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
get_cosmos_container <- function(database, ...)
{
    UseMethod("get_cosmos_container")
}

#' @rdname cosmos_container
#' @export
get_cosmos_container.cosmos_database <- function(database, name, ...)
{
    path <- file.path("colls", name)
    res <- do_cosmos_op(database, path, "colls", path, ...)
    obj <- process_cosmos_response(res)
    obj$database <- database
    class(obj) <- "cosmos_container"
    obj
}


#' @rdname cosmos_container
#' @export
create_cosmos_container <- function(database, ...)
{
    UseMethod("create_cosmos_container")
}

#' @rdname cosmos_container
#' @export
create_cosmos_container.cosmos_database <- function(database, name, partition_key, partition_version=2,
    autoscale_maxRUs=NULL, manual_RUs=NULL, headers=list(), ...)
{
    if(!is.null(manual_RUs))
        headers$`x-ms-offer-throughput` <- manual_RUs
    if(!is.null(autoscale_maxRUs))
        headers$`x-ms-cosmos-offer-autopilot-settings` <- jsonlite::toJSON(autoscale_maxRUs)

    body <- list(
        id=name,
        partitionKey=list(
            paths=list(paste0("/", partition_key)),
            kind="Hash",
            version=partition_version
        )
    )

    res <- do_cosmos_op(database, "colls", "colls", "", headers=headers, body=body, encode="json",
                        http_verb="POST", ...)
    obj <- process_cosmos_response(res)
    obj$database <- database
    class(obj) <- "cosmos_container"
    obj

}


#' @rdname cosmos_container
#' @export
delete_cosmos_container <- function(database, ...)
{
    UseMethod("delete_cosmos_container")
}

#' @rdname cosmos_container
#' @export
delete_cosmos_container.cosmos_database <- function(database, name, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, name, "container"))
        return(invisible(NULL))

    path <- file.path("colls", name)
    res <- do_cosmos_op(database, path, "colls", path, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res))
}

#' @rdname cosmos_container
#' @export
delete_cosmos_container.cosmos_container <- function(database, ...)
{
    delete_cosmos_container(database$database, database$id, ...)
}


#' @rdname cosmos_container
#' @export
list_cosmos_containers <- function(database, ...)
{
    UseMethod("list_cosmos_containers")
}

#' @rdname cosmos_container
#' @export
list_cosmos_containers.cosmos_database <- function(database, ...)
{
    res <- do_cosmos_op(database, "colls", "colls", "", ...)
    AzureRMR::named_list(lapply(process_cosmos_response(res)$DocumentCollections, function(obj)
    {
        obj$database <- database
        structure(obj, class="cosmos_container")
    }), "id")
}


#' @rdname cosmos_container
#' @export
get_partition_key <- function(container)
{
    UseMethod("get_partition_key")
}

#' @rdname cosmos_container
#' @export
get_partition_key.cosmos_container <- function(container)
{
    key <- container$partitionKey$paths
    if(is.character(key))
        sub("^/", "", key)
    else NULL
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

#' @rdname do_cosmos_op
#' @export
do_cosmos_op.cosmos_container <- function(object, path="", resource_type="colls", resource_link="", ...)
{
    full_path <- full_reslink <-  file.path("dbs", object$database$id, "colls", object$id)
    if(nchar(path) > 0)
        full_path <- file.path(full_path, path)
    if(nchar(resource_link) > 0)
        full_reslink <- file.path(full_reslink, resource_link)
    call_cosmos_endpoint(object$database$endpoint, full_path, resource_type, full_reslink, ...)
}


#' @rdname cosmos_container
#' @export
list_key_values <- function(container)
{
    UseMethod("get_key_values")
}

#' @rdname cosmos_container
#' @export
list_key_values.cosmos_container <- function(container)
{
    key <- get_partition_key(container)[1]
    qry <- sprintf("select distinct value %s.%s from %s", container$id, key, container$id)
    lst <- suppressMessages(query_documents(container, qry, by_physical_partition=TRUE))
    unique(unlist(lst))
}


get_partition_physical_ids <- function(container, ...)
{
    UseMethod("get_partition_physical_ids")
}

#' @export
get_partition_physical_ids.cosmos_container <- function(container, id_only=TRUE, ...)
{
    res <- do_cosmos_op(container, "pkranges", "pkranges", "",
        headers=list(`x-ms-documentdb-query-enablecrosspartition`=TRUE))
    lst <- process_cosmos_response(res)
    if(id_only)
        sapply(lst$PartitionKeyRanges, `[[`, "id")
    else lst$PartitionKeyRanges
}

