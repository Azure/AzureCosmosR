#' Operations with azure tables
#'
#' @param endpoint An object of class `table_endpoint`.
#' @param name The name of a table in a storage account.
#' @param confirm For deleting a table, whether to ask for confirmation.
#' @param ... Other arguments passed to lower-level functions.
#' @rdname azure_table
#' @details
#' These methods are for accessing and managing tables within a storage account.
#' @seealso
#' [table_endpoint], [table_entity]
#' @export
azure_table <- function(endpoint, ...)
{
    UseMethod("azure_table")
}

#' @rdname azure_table
#' @export
azure_table.table_endpoint <- function(endpoint, name, ...)
{
    structure(list(endpoint=endpoint, name=name), class="azure_table")
}


#' @rdname azure_table
#' @export
list_azure_tables <- function(endpoint, ...)
{
    UseMethod("list_azure_tables")
}

#' @rdname azure_table
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
    named_list(lapply(val, function(x) azure_table(endpoint, x$TableName)))
}


#' @rdname azure_table
#' @export
create_azure_table <- function(endpoint, ...)
{
    UseMethod("create_azure_table")
}

#' @rdname azure_table
#' @export
create_azure_table.table_endpoint <- function(endpoint, name, ...)
{
    res <- call_table_endpoint(endpoint, "Tables", body=list(TableName=name), ..., http_verb="POST")
    azure_table(endpoint, res$TableName)
}


#' @rdname azure_table
#' @export
delete_azure_table <- function(endpoint, ...)
{
    UseMethod("delete_azure_table")
}

#' @rdname azure_table
#' @export
delete_azure_table.table_endpoint <- function(endpoint, name, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, name, "table"))
        return(invisible(NULL))
    path <- sprintf("Tables('%s')", name)
    invisible(call_table_endpoint(endpoint, path, http_verb="DELETE"))
}

#' @rdname azure_table
#' @export
delete_azure_table.azure_table <- function(endpoint, ...)
{
    delete_azure_table(endpoint$endpoint, endpoint$name, ...)
}


#' @export
print.azure_table <- function(x, ...)
{
    cat("Azure table '", x$name, "'\n",
        sep = "")
    url <- httr::parse_url(x$endpoint$url)
    url$path <- x$name
    cat(sprintf("URL: %s\n", httr::build_url(url)))
    if (!is_empty(x$endpoint$key))
        cat("Access key: <hidden>\n")
    else cat("Access key: <none supplied>\n")
    if (!is_empty(x$endpoint$token)) {
        cat("Azure Active Directory access token:\n")
        print(x$endpoint$token)
    }
    else cat("Azure Active Directory access token: <none supplied>\n")
    if (!is_empty(x$endpoint$sas))
        cat("Account shared access signature: <hidden>\n")
    else cat("Account shared access signature: <none supplied>\n")
    cat(sprintf("Storage API version: %s\n", x$endpoint$api_version))
    invisible(x)
}

