#' @export
get_container <- function(database, ...)
{
    UseMethod("get_container")
}

#' @export
get_container.cosmos_database <- function(database, name, ...)
{
    path <- file.path("colls", name)
    res <- do_cosmos_op(database, path, "colls", path, ...)
    obj <- process_cosmos_response(res, ...)
    obj$database <- database
    class(obj) <- "cosmos_container"
    obj
}


#' @export
create_container <- function(database, ...)
{
    UseMethod("create_container")
}

#' @export
create_container.cosmos_database <- function(database, name, partition_key, partition_version=1,
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
    obj <- process_cosmos_response(res, ...)
    obj$database <- database
    class(obj) <- "cosmos_container"
    obj

}


#' @export
delete_container <- function(database, ...)
{
    UseMethod("delete_container")
}

#' @export
delete_container.cosmos_database <- function(database, name, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, name, "container"))
        return(invisible(NULL))

    path <- file.path("colls", name)
    res <- do_cosmos_op(database, path, "colls", path, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res, ...))
}

#' @export
delete_container.cosmos_container <- function(database, ...)
{
    delete_container(database$database, database$id, ...)
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
do_cosmos_op.cosmos_container <- function(object, path="", resource_type="colls", resource_link="", ...)
{
    full_path <- full_reslink <-  file.path("dbs", object$database$id, "colls", object$id)
    if(nchar(path) > 0)
        full_path <- file.path(full_path, path)
    if(nchar(resource_link) > 0)
        full_reslink <- file.path(full_reslink, resource_link)
    call_cosmos_endpoint(object$database$endpoint, full_path, resource_type, full_reslink, ...)
}

