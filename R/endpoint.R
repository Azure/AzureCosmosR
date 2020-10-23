#' @export
cosmos_endpoint <- function(host, key, key_type=c("master", "resource"),
                            api_version=getOption("azure_cosmosdb_api_version"))
{
    obj <- list(
        host=httr::parse_url(host),
        key=list(value=unname(key), type=match.arg(key_type)),
        api_version=api_version
    )
    class(obj) <- "cosmos_endpoint"
    obj
}

#' @export
print.cosmos_endpoint <- function(x, ...)
{
    cat("Cosmos DB SQL endpoint\n")
    cat("Host:", httr::build_url(x$host), "\n")
    invisible(x)
}


#' @export
call_cosmos_endpoint <- function(endpoint, path, resource_type, options=list(), headers=list(), body=NULL,
    http_verb=c("GET", "DELETE", "PUT", "POST", "PATCH", "HEAD"),
    num_retries=10, ...)
{
    url <- endpoint$host
    url$path <- gsub("/{2,}", "/", URLencode(enc2utf8(path)))
    if(!is_empty(options))
        url$query <- options

    headers$`x-ms-version` <- endpoint$api_version
    http_verb <- match.arg(http_verb)
    for(r in seq_len(num_retries))
    {
        now <- httr::http_date(Sys.time())
        headers$`x-ms-date` <- now
        headers$Authorization <- sign_cosmos_request(
            endpoint$key,
            http_verb,
            resource_type,
            path,
            now
        )
        response <- tryCatch(httr::VERB(http_verb, url, do.call(httr::add_headers, headers), body=body, ...),
                             error=function(e) e)
        if(!retry_transfer(response))
            break
        if(inherits(response, "response"))
            delay <- headers(response)$`x-ms-retry-after-ms`
        delay <- if(!is.null(delay)) as.numeric(delay)/1000 else 1
        Sys.sleep(delay)
    }
    if(inherits(response, "error"))
        stop(response)

    response
}

retry_transfer <- function(response)
{
    UseMethod("retry_transfer")
}

retry_transfer.error <- function(response)
{
    # retry on curl errors and http 429 responses
    grepl("curl", deparse(response$call[[1]]), fixed=TRUE) &&
        !grepl("Could not resolve host", response$message, fixed=TRUE)
}

retry_transfer.response <- function(response)
{
    httr::status_code(response) == 429
}


#' @export
process_cosmos_response <- function(response, http_status_handler=c("stop", "warn", "message", "pass"),
    return_headers=(response$request$method == "HEAD"))
{
    http_status_handler <- match.arg(http_status_handler)
    if(http_status_handler == "pass")
        return(response)

    handler <- get(paste0(http_status_handler, "_for_status"), getNamespace("httr"))
    handler(response, cosmos_error_message(response))

    if(return_headers)
        return(unclass(httr::headers(response)))

    httr::content(response, simplifyVector=TRUE)
}

cosmos_error_message <- function(response)
{
    paste0("complete Cosmos DB operation. Message:\n", sub("\\.$", "", httr::content(response)$message))
}
