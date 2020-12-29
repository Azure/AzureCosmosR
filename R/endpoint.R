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
do_cosmos_op <- function(object, ...)
{
    UseMethod("do_cosmos_op")
}

#' @export
do_cosmos_op.cosmos_endpoint <- function(object, ...)
{
    call_cosmos_endpoint(object, ...)
}


#' @export
call_cosmos_endpoint <- function(endpoint, path, resource_type, resource_link,
    options=list(), headers=list(), body=NULL, do_continuations=TRUE, ...)
{
    headers$`x-ms-version` <- endpoint$api_version
    url <- endpoint$host
    url$path <- gsub("/{2,}", "/", URLencode(enc2utf8(path)))
    if(!AzureRMR::is_empty(options))
        url$query <- options

    # repeat until no more continuations
    reslst <- list()
    repeat
    {
        response <- do_request(url, endpoint$key, resource_type, resource_link, headers, body, ...)
        if(inherits(response, "error"))
            stop(response)

        reslst <- c(reslst, list(response))
        response_headers <- httr::headers(response)
        if(do_continuations && !is.null(response_headers$`x-ms-continuation`))
            headers$`x-ms-continuation` <- response_headers$`x-ms-continuation`
        else
        {
            if(!is.null(response_headers$`x-ms-continuation`))
                attr(reslst[[1]], "x-ms-continuation" <- response_headers$`x-ms-continuation`)
            break
        }
    }

    if(length(reslst) == 1)
        reslst[[1]]
    else reslst
}


do_request <- function(url, key, resource_type, resource_link, headers=list(), body=NULL,
    http_verb=c("GET", "DELETE", "PUT", "POST", "PATCH", "HEAD"), num_retries=10,
    ...)
{
    http_verb <- match.arg(http_verb)
    for(r in seq_len(num_retries))
    {
        now <- httr::http_date(Sys.time())
        headers$`x-ms-date` <- now
        headers$Authorization <- sign_cosmos_request(
            key,
            http_verb,
            resource_type,
            resource_link,
            now
        )
        response <- tryCatch(httr::VERB(http_verb, url, do.call(httr::add_headers, headers), body=body, ...),
                             error=function(e) e)
        if(!retry_transfer(response))  # retry on curl errors (except host not found) and http 429 responses
            break
        delay <- if(inherits(response, "response"))
        {
            delay <- httr::headers(response)$`x-ms-retry-after-ms`
            if(!is.null(delay)) as.numeric(delay)/1000 else 1
        }
        else 1
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
    grepl("curl", deparse(response$call[[1]]), fixed=TRUE) &&
        !grepl("Could not resolve host", response$message, fixed=TRUE)
}

retry_transfer.response <- function(response)
{
    httr::status_code(response) == 429
}


#' @export
process_cosmos_response <- function(response, ...)
{
    UseMethod("process_cosmos_response")
}

#' @export
process_cosmos_response.response <- function(response, http_status_handler=c("stop", "warn", "message", "pass"),
    return_headers=NULL, simplify=FALSE, ...)
{
    http_status_handler <- match.arg(http_status_handler)
    if(http_status_handler == "pass")
        return(response)

    handler <- get(paste0(http_status_handler, "_for_status"), getNamespace("httr"))
    handler(response, cosmos_error_message(response))
    if(is.null(return_headers))
        return_headers <- response$request$method == "HEAD"

    if(return_headers)
        unclass(httr::headers(response))
    else httr::content(response, simplifyVector=TRUE, simplifyDataFrame=simplify)
}


#' @export
process_cosmos_response.list <- function(response, http_status_handler=c("stop", "warn", "message", "pass"),
    return_headers=NULL, simplify=FALSE, ...)
{
    if(!inherits(response[[1]], "response"))
        stop("Expecting a list of response objects", call.=FALSE)

    http_status_handler <- match.arg(http_status_handler)
    if(http_status_handler == "pass")
        return(response)

    lapply(response, process_cosmos_response, http_status_handler=http_status_handler, return_headers=return_headers,
           simplify=simplify)
}


cosmos_error_message <- function(response)
{
    paste0("complete Cosmos DB operation. Message:\n", sub("\\.$", "", httr::content(response)$message))
}
