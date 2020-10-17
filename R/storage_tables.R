list_azure_tables <- function(endpoint, metadata=c("none", "minimal", "full"))
{
    metadata <- match.arg(metadata)
    accept <- switch(metadata,
        "none"="application/json;odata=nometadata",
        "minimal"="application/json;odata=minimalmetadata",
        "full"="application/json;odata=fullmetadata")
    res <- call_storage_endpoint(endpoint, "Tables", headers=list(Accept=accept), http_status_handler="pass")
    stop_for_status(res)
    heads <- httr::headers(res)
    if(!is.null(heads[["x-ms-continuation-NextTableName"]]))
        res <- call_storage_endpoint(endpoint, "Tables",
            options=list(NextTableName=heads[["x-ms-continuation-NextTableName"]]),
            http_status_handler="pass")
}


#' @export
create_azure_table <- function(endpoint, ...)
{
    UseMethod("create_azure_table")
}

#' @export
create_azure_table.table_endpoint <- function(endpoint, name, metadata=c("none", "minimal", "full"), ...)
{
    res <- call_table_endpoint(endpoint, "Tables", body=list(TableName=name), metadata=metadata, http_verb="POST")
    res$endpoint <- endpoint
    class(res) <- "azure_table"
    res
}


#' @export
delete_azure_table <- function(endpoint, ...)
{
    UseMethod("delete_azure_table")
}

#' @export
delete_azure_table.table_endpoint <- function(endpoint, name, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, name, "table"))
        return(invisible(NULL))
    path <- sprintf("Tables('%s')", name)
    invisible(call_table_endpoint(endpoint, path, http_verb="DELETE"))
}

#' @export
delete_azure_table.azure_table <- function(endpoint, ...)
{
    delete_azure_table(endpoint$endpoint, endpoint$TableName, ...)
}


#' @export
azure_table <- function(endpoint, ...)
{
    UseMethod("azure_table")
}

#' @export
azure_table.table_endpoint <- function(endpoint, name, metadata=NULL, type=NULL, id=NULL, editlink=NULL, ...)
{
    obj <- list(
        odata.metadata=metadata,
        odata.type=type,
        odata.id=id,
        odata.editLink=editlink,
        TableName=name,
        endpoint=endpoint
    )
    obj <- obj[!sapply(obj, is.null)]
    class(obj) <- "azure_table"
    obj
}


