sign_sha256 <- function(string, key)
{
    openssl::base64_encode(openssl::sha256(charToRaw(string), openssl::base64_decode(key)))
}

sign_cosmos_request <- function(key, verb, resource_type, resource_link, date)
{
    if(inherits(date, "POSIXt"))
        date <- httr::http_date(date)
    string_to_sign <- paste(
        tolower(verb),
        tolower(resource_type),
        resource_link,
        tolower(date),
        "",
        "",
        sep="\n"
    )
    sig <- sign_sha256(string_to_sign, key$value)
    utils::URLencode(sprintf("type=%s&ver=1.0&sig=%s", key$type, sig), reserved=TRUE)
}

