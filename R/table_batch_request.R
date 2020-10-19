BatchOperation <- R6::R6Class("BatchOperation",

public=list(
    endpoint=NULL,
    path=NULL,
    options=NULL,
    headers=NULL,
    method=NULL,
    body=NULL,
    http_version="1.1",

    initialize=function(endpoint, path, options=list(), headers=list(), body=NULL,
        metadata=c("none", "minimal", "full"),
        http_verb=c("GET", "PUT", "POST", "PATCH", "DELETE", "HEAD"))
    {
        accept <- if(!is.null(metadata))
        {
            metadata <- match.arg(metadata)
            switch(match.arg(metadata),
                "none"="application/json;odata=nometadata",
                "minimal"="application/json;odata=minimalmetadata",
                "full"="application/json;odata=fullmetadata")
        }
        else NULL

        self$endpoint <- endpoint
        self$path <- path
        self$options <- options
        self$headers <- utils::modifyList(headers, list(Accept=accept, DataServiceVersion="3.0;NetFx"))
        self$method <- http_verb
        self$body <- body
    },

    serialize=function()
    {
        url <- httr::parse_url(self$endpoint$url)
        url$path <- self$path
        url$query <- self$options

        preamble <- c(
            "Content-Type: application/http",
            "Content-Transfer-Encoding: binary",
            "",
            paste0(self$method, " ", httr::build_url(url), " HTTP/", self$http_version),
            paste0(names(self$headers), ": ", self$headers),
            if(!is.null(self$body)) "Content-Type: application/json"
        )

        if(is.null(self$body))
            preamble
        else if(!is.character(self$body))
        {
            body <- jsonlite::toJSON(self$body, auto_unbox=TRUE, null="null")
            # special-case treatment for 1-row dataframes
            if(is.data.frame(self$body) && nrow(self$body) == 1)
                body <- substr(body, 2, nchar(body) - 1)
            c(preamble, "", body)
        }
        else c(preamble, "", self$body)
    }
))


create_batch_operation <- function(endpoint, path, options=list(), headers=list(), body=NULL,
    metadata=c("none", "minimal", "full"), http_verb=c("GET", "PUT", "POST", "PATCH", "DELETE", "HEAD"))
{
    BatchOperation$new(endpoint, path, options, headers, body, metadata, http_verb)
}


send_batch_request <- function(endpoint, operations)
{
    # batch REST API only supports 1 changeset per batch, and is unlikely to change
    batch_bound <- paste0("batch_", uuid::UUIDgenerate())
    changeset_bound <- paste0("changeset_", uuid::UUIDgenerate())
    headers <- list(`Content-Type`=paste0("multipart/mixed; boundary=", batch_bound))

    batch_preamble <- c(
        paste0("--", batch_bound),
        paste0("Content-Type: multipart/mixed; boundary=", changeset_bound),
        ""
    )
    batch_postscript <- c(
        "",
        paste0("--", changeset_bound, "--"),
        paste0("--", batch_bound, "--")
    )
    serialized <- lapply(operations, function(op) c(paste0("--", changeset_bound), op$serialize()))
    body <- paste0(c(batch_preamble, unlist(serialized), batch_postscript), collapse="\n")
    headers$`Content-Length` <- nchar(body)

    res <- call_table_endpoint(endpoint, "$batch", headers=headers, body=body, encode="raw",
        http_verb="POST")
    invisible(rawToChar(res))
}
