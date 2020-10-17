table_endpoint <- function(endpoint, key=NULL, token=NULL, sas=NULL,
                           api_version=getOption("azure_storage_api_version"))
{
    if(!is_endpoint_url(endpoint, "table"))
        warning("Not a recognised table endpoint", call.=FALSE)
    obj <- list(url=endpoint, key=key, token=token, sas=sas, api_version = api_version)
    class(obj) <- c("table_endpoint", "storage_endpoint")
    obj
}


call_table_endpoint <- function(endpoint, path, options=list(), headers=list(), body=NULL, ...,
    metadata=c("none", "minimal", "full"))
{
    metadata <- match.arg(metadata)
    accept <- switch(metadata,
        "none"="application/json;odata=nometadata",
        "minimal"="application/json;odata=minimalmetadata",
        "full"="application/json;odata=fullmetadata")
    headers <- utils::modifyList(headers, list(Accept=accept))

    if(is.list(body))
    {
        body <- jsonlite::toJSON(body, auto_unbox=TRUE, null="null")
        headers$`Content-Length` <- nchar(body)
        headers$`Content-Type` <- "application/json"
    }
    call_storage_endpoint(endpoint, path=path, options=options, body=body, headers=headers, ...)
}



