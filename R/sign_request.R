#' @export
sign_request.table_endpoint <- function(endpoint, verb, url, headers, api, ...)
{
    make_sig <- function(key, verb, acct_name, resource, headers)
    {
        names(headers) <- tolower(names(headers))
        sigstr <- paste(verb,
            as.character(headers[["content-md5"]]),
            as.character(headers[["content-type"]]),
            as.character(headers[["date"]]),
            resource, sep = "\n")
        sigstr <- sub("\n$", "", sigstr)
        paste0("SharedKey ", acct_name, ":", sign_sha256(sigstr, key))
    }

    acct_name <- sub("\\..+$", "", url$host)
    resource <- paste0("/", acct_name, "/", url$path)
    resource <- gsub("//", "/", resource)
    if (is.null(headers$date) || is.null(headers$Date))
        headers$date <- httr::http_date(Sys.time())
    if (is.null(headers$`x-ms-version`))
        headers$`x-ms-version` <- api

    sig <- make_sig(endpoint$key, verb, acct_name, resource, headers)
    utils::modifyList(headers, list(Host=url$host, Authorization=sig))
}

