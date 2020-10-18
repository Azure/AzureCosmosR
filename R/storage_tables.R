#' @export
list_azure_tables <- function(endpoint, ...)
{
    UseMethod("list_azure_tables")
}

#' @export
list_azure_tables.table_endpoint <- function(endpoint, ...)
{
    opts <- list()
    val <- list()
    repeat
    {
        res <- call_table_endpoint(endpoint, "Tables", options=opts, http_status_handler="pass")
        httr::stop_for_status(res, storage_error_message(res))
        heads <- httr::headers(res)
        res <- httr::content(res)
        val <- c(val, res$value)

        if(is.null(heads$`x-ms-continuation-NextTableName`))
            break
        opts$NextTableName <- heads$`x-ms-continuation-NextTableName`
    }
    AzureRMR::named_list(lapply(val, function(x) azure_table(endpoint, x$TableName)))
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
    azure_table(endpoint, res$TableName)
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
    delete_azure_table(endpoint$endpoint, endpoint$name, ...)
}


#' @export
azure_table <- function(endpoint, ...)
{
    UseMethod("azure_table")
}

#' @export
azure_table.table_endpoint <- function(endpoint, name, ...)
{
    structure(list(endpoint=endpoint, name=name), class="azure_table")
}


