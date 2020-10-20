#' Table storage endpoint
#'
#' Table storage endpoint object, and method to call it.
#'
#' @param endpoint For `table_endpoint`, the URL (hostname) of the table storage endpoint: for the Azure public cloud, this will be of the form `https://{account-name}.table.core.windows.net`. For `call_table_endpoint`, an object of class `table_endpoint`.
#' @param key The access key for the storage account.
#' @param token An Azure Active Directory (AAD) authentication token. Not used for table storage.
#' @param sas A shared access signature (SAS) for the account.
#' @param api_version The storage API version to use when interacting with the host. Defaults to "2019-07-07".
#' @param path For `call_table_endpoint`, the path component of the endpoint call.
#' @param options For `call_table_endpoint`, a named list giving the query parameters for the operation.
#' @param headers For `call_table_endpoint`, a named list giving any additional HTTP headers to send to the host. AzureCosmosR will handle authentication details, so you don't have to specify these here.
#' @param body For `call_table_endpoint`, the request body for a PUT/POST/PATCH call.
#' @param metadata For `call_table_endpoint`, the level of ODATA metadata to include in the response.
#' @param ... For `call_table_endpoint`, further arguments passed to `AzureStor::call_storage_endpoint` and `httr::VERB`.
#'
#' @return
#' An object of class `table_endpoint`, inheriting from `storage_endpoint`. This is the analogue of the `blob_endpoint`, `file_endpoint` and `adls_endpoint` classes provided by the AzureStor package.
#'
#' @seealso
#' [azure_table], [table_entity]
#'
#' [Table service REST API reference](https://docs.microsoft.com/en-us/rest/api/storageservices/table-service-rest-api)
#' @rdname table_endpoint
#' @export
table_endpoint <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                           api_version=getOption("azure_storage_api_version"))
{
    if(!is_endpoint_url(endpoint, "table"))
        warning("Not a recognised table endpoint", call.=FALSE)
    if(!is.null(token))
    {
        warning("Table storage does not use Azure Active Directory authentication")
        token <- NULL
    }
    obj <- list(url=endpoint, key=key, token=token, sas=sas, api_version=api_version)
    class(obj) <- c("table_endpoint", "storage_endpoint")
    obj
}


#' @rdname table_endpoint
#' @export
call_table_endpoint <- function(endpoint, path, options=list(), headers=list(), body=NULL, ...,
    metadata=c("none", "minimal", "full"))
{
    accept <- if(!is.null(metadata))
    {
        metadata <- match.arg(metadata)
        switch(metadata,
            "none"="application/json;odata=nometadata",
            "minimal"="application/json;odata=minimalmetadata",
            "full"="application/json;odata=fullmetadata")
    }
    else NULL
    headers <- utils::modifyList(headers, list(Accept=accept, DataServiceVersion="3.0;NetFx"))

    if(is.list(body))
    {
        body <- jsonlite::toJSON(body, auto_unbox=TRUE, null="null")
        headers$`Content-Length` <- nchar(body)
        headers$`Content-Type` <- "application/json"
    }
    call_storage_endpoint(endpoint, path=path, options=options, body=body, headers=headers, ...)
}

