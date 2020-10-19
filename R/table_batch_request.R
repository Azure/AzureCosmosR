BatchOperation <- R6::R6Class("BatchOperation",

public=list(
    endpoint=NULL,
    path=NULL,
    options=NULL,
    headers=NULL,
    method=NULL,
    body=NULL,

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
        self$headers <- utils::modifyList(headers, list(Accept=accept, DataServiceVersion="3.0"))
        self$method <- http_verb
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
            paste0(names(self$headers), ": ", self$headers)
        )

        if(is.null(self$body))
            preamble
        else if(!is.character(self$body))
            c(preamble, "", jsonlite::toJSON(self$body, auto_unbox=TRUE, null="null"))
        else c(preamble, "", self$body)
    }
))


BatchRequest <- R6::R6Class("BatchRequest",

public=list(
    endpoint=NULL,
    changesets=list(),

    initialize=function(endpoint, changesets)
    {
        self$endpoint <- endpoint
        self$changesets <- changesets
    },

    send=function()
    {
        batch_bound <- paste0("batch_", uuid::UUIDgenerate())
        changeset_bound <- paste0("req_", uuid::UUIDgenerate())
        headers <- list(`Content-Type`=paste0("multipart/mixed; boundary=", batch_bound))
        call_table_endpoint(endpoint, "$batch", headers=headers, body=body, encode="raw", http_verb="POST")
    }
))




create_batch_operation <- function(endpoint, path, options=list(), headers=list(), body=NULL,
    metadata=c("none", "minimal", "full"), http_verb=c("GET", "PUT", "POST", "PATCH", "DELETE", "HEAD"))
{
    BatchOperation$new(endpoint, path, options, headers, body, metadata, http_verb)
}


send_batch_request <- function(endpoint, operations, ...)
{
    # batch REST API only supports 1 changeset per batch, and is unlikely to change
    batch_bound <- paste0("--batch_", uuid::UUIDgenerate())
    changeset_bound <- paste0("--changeset_", uuid::UUIDgenerate())
    headers <- list(`Content-Type`=paste0("multipart/mixed; boundary=", batch_bound))

    batch_preamble <- c(
        batch_bound,
        paste0("Content-Type: multipart/mixed; boundary=", changeset_bound)
    )
    batch_postscript <- c(
        paste0(changeset_bound, "--"),
        batch_bound
    )
    reqs <- lapply(requests, function(req) c(changeset_bound, req$serialize()))
    body <- c(batch_preamble, unlist(reqs), batch_postscript)

    invisible(call_table_endpoint(endpoint, "$batch", headers=headers, body=paste0(body, collapse="\n"), encode="raw",
        http_verb="POST"))
}
