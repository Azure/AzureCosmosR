#' Client endpoint for Azure Cosmos DB core API
#'
#' @param host For `cosmos_endpoint`, the host URL for the endpoint. Typically of the form `https://{account-name}.documents.azure.com:443/` (note the port number).
#' @param key For `cosmos_endpoint`, a string containing the password for the endpoint. This can be either a master key, a resource token, or an AAD token.
#' @param key_type For `cosmos_endpoint`, the type of the key, either "master" or "resource" or "aad".
#' @param api_version For `cosmos_endpoint`, the API version to use.
#' @param endpoint For `call_cosmos_endpoint`, a Cosmos DB endpoint object, as returned by `cosmos_endpoint`.
#' @param path For `call_cosmos_endpoint`, the path in the URL for the endpoint call.
#' @param resource_type For `call_cosmos_endpoint`, the type of resource: for example, "dbs" for a database, "colls" for a collection (container), "docs" for a document, etc.
#' @param resource_link For `call_cosmos_endpoint`, a string to pass to the API for authorization purposes. See the Cosmos DB API documentation for more information.
#' @param options For `call_cosmos_endpoint`, query options to include in the request URL.
#' @param headers For `call_cosmos_endpoint`, any HTTP headers to include in the request. You don't need to include authorization headers as `call_cosmos_endpoint` will take care of the details.
#' @param body For `call_cosmos_endpoint`, the body of the request if any.
#' @param encode For `call_cosmos_endpoint`, the encoding (really content-type) of the request body. The Cosmos DB REST API uses JSON, so there should rarely be a need to change this argument.
#' @param do_continuations For `call_cosmos_endpoint`, whether to automatically handle paged responses. If FALSE, only the initial response is returned.
#' @param http_verb For `call_cosmos_endpoint`, the HTTP verb for the request. One of "GET", "POST", "PUT", "PATCH", "HEAD" or "DELETE".
#' @param num_retries For `call_cosmos_endpoint`, how many times to retry a failed request. Useful for dealing with rate limiting issues.
#' @param response For `process_cosmos_response`, the returned object from a `call_cosmos_endpoint` call. This will be either a single httr request object, or a list of such objects.
#' @param http_status_handler For `process_cosmos_response`, the R handler for the HTTP status code of the response.  "stop", "warn" or "message" will call the corresponding handlers in httr, while "pass" ignores the status code. The latter is primarily useful for debugging purposes.
#' @param return_headers For `process_cosmos_response`, whether to return the headers from the response object(s), as opposed to the body. Defaults to TRUE if the original endpoint call was a HEAD request, and FALSE otherwise.
#' @param simplify For `process_cosmos_response`, whether to convert arrays of objects into data frames via the `simplifyDataFrame` argument to [jsonlite::fromJSON].
#' @param ... Arguments passed to lower-level functions.
#' @details
#' These functions are the basis of the SQL API client framework provided by AzureCosmosR. The `cosmos_endpoint` function returns a client object, which can then be passed to other functions for querying databases and containers. The `call_cosmos_endpoint` function sends calls to the REST endpoint, the results of which are then processed by `process_cosmos_response`.
#'
#' In most cases, you should not have to use `call_cosmos_endpoint` directly. Instead, use `do_cosmos_op` which provides a slightly higher-level interface to the API, by providing sensible defaults for the `resource_type` and`resource_link` arguments and partially filling in the request path.
#'
#' As an alternative to AzureCosmosR, you can also use the ODBC protocol to interface with the SQL API. By installing a suitable ODBC driver, you can then talk to Cosmos DB in a manner similar to other SQL databases. An advantage of the ODBC interface is that it fully supports cross-partition queries, unlike the REST API. A disadvantage is that it does not support nested document fields; functions like `array_contains()` cannot be used, and attempts to reference arrays and objects may return incorrect results.
#'
#' Note that AzureCosmosR is a framework for communicating directly with the _core_ Cosmos DB client API, also known as the "SQL" API. Cosmos DB provides other APIs as options when creating an account, such as Cassandra, MongoDB, table storage and Gremlin. These APIs are not supported by AzureCosmosR, but you can use other R packages for working with them. For example, you can use AzureTableStor to work with the table storage API, or mongolite to work with the MongoDB API.
#' @return
#' For `cosmos_endpoint`, an object of S3 class `cosmos_endpoint`.
#'
#' For `call_cosmos_endpoint`, either a httr response object, or a list of such responses (if a paged query, and `do_continuations` is TRUE).
#'
#' For `process_cosmos_response` and a single response object, the content of the response. This can be either the parsed response body (if `return_headers` is FALSE) or the headers (if `return_headers` is TRUE).
#'
#' For `process_cosmos_response` and a list of response objects, a list containing the individual contents of each response.
#' @seealso
#' [do_cosmos_op], [cosmos_database], [cosmos_container], [az_cosmosdb]
#'
#' [httr::VERB], which is what carries out the low-level work of sending the HTTP request.
#' @examples
#' \dontrun{
#'
#' endp <- cosmos_endpoint("https://myaccount.documents.azure.com:443/", key="mykey")
#'
#' # properties for the Cosmos DB account
#' call_cosmos_endpoint(endp, "", "", "") %>%
#'     process_cosmos_response()
#'
#' }
#' @rdname cosmos_endpoint
#' @export
cosmos_endpoint <- function(host, key, key_type=c("master", "resource", "aad"),
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
    cat("Key type:", x$key$type, "\n")
    cat("Api version:", x$api_version, "\n")
    invisible(x)
}


#' @rdname cosmos_endpoint
#' @export
call_cosmos_endpoint <- function(endpoint, path, resource_type, resource_link,
    options=list(), headers=list(), body=NULL, encode="json", do_continuations=TRUE,
    http_verb=c("GET", "DELETE", "PUT", "POST", "PATCH", "HEAD"), num_retries=10, ...)
{
    http_verb <- match.arg(http_verb)
    headers$`x-ms-version` <- endpoint$api_version
    url <- endpoint$host
    url$path <- gsub("/{2,}", "/", utils::URLencode(enc2utf8(path)))
    if(!is_empty(options))
        url$query <- options

    # repeat until no more continuations
    reslst <- list()
    repeat
    {
        response <- do_request(url, endpoint$key, resource_type, resource_link, headers, body,
                               http_verb=http_verb, num_retries=num_retries, ...)
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


do_request <- function(url, key, resource_type, resource_link, headers=list(), body=NULL, encode="json",
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
     
        response <- tryCatch(httr::VERB(http_verb, url, do.call(httr::add_headers, headers),
                                        body=body, encode=encode, ...),
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


#' @rdname cosmos_endpoint
#' @export
process_cosmos_response <- function(response, ...)
{
    UseMethod("process_cosmos_response")
}

#' @rdname cosmos_endpoint
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


#' @rdname cosmos_endpoint
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
