#' Batch transactions for table storage
#'
#' @param endpoint A table storage endpoint, of class `table_endpoint`.
#' @param path The path component of the operation.
#' @param options A named list giving the query parameters for the operation.
#' @param headers A named list giving any additional HTTP headers to send to the host. AzureCosmosR will handle authentication details, so you don't have to specify these here.
#' @param body The request body for a PUT/POST/PATCH operation.
#' @param metadata The level of ODATA metadata to include in the response.
#' @param http_verb The HTTP verb (method) for the operation.
#' @param operations For `do_batch_transaction`, a list of individual operations to be batched up.
#' @param batch_status_handler For `do_batch_transaction`, what to do if one or more of the batch operations fails. The default is to signal a warning and return a list of response objects, from which the details of the failure(s) can be determined. Set this to "pass" to ignore the failure.
#'
#' @details
#' Table storage supports batch transactions on entities that are in the same table and belong to the same partition group. Batch transactions are also known as _entity group transactions_.
#'
#' You can use `create_batch_operation` to produce an object corresponding to a single table storage operation, such as inserting, deleting or updating an entity. Multiple such objects can then be passed to `do_batch_transaction`, which will carry them out as a single atomic transaction.
#'
#' Note that batch transactions are subject to some limitations imposed by the REST API:
#' - All entities subject to operations as part of the transaction must have the same `PartitionKey` value.
#' - An entity can appear only once in the transaction, and only one operation may be performed against it.
#' - The transaction can include at most 100 entities, and its total payload may be no more than 4 MB in size.
#'
#' @return
#' `create_batch_operation` returns an object of class `batch_operation`.
#'
#' `do_batch_transaction` returns a list of objects of class `batch_operation_response`, representing the results of each individual operation. Each object contains elements named `status`, `headers` and `body` containing the respective parts of the response. Note that the number of returned objects may be smaller than the number of operations in the batch, if the transaction failed.
#' @seealso
#' [import_table_entities], which uses (multiple) batch transactions under the hood
#'
#' [Performing entity group transactions](https://docs.microsoft.com/en-us/rest/api/storageservices/performing-entity-group-transactions)
#' @rdname table_batch
#' @export
create_batch_operation <- function(endpoint, path, options=list(), headers=list(), body=NULL,
    metadata=c("none", "minimal", "full"), http_verb=c("GET", "PUT", "POST", "PATCH", "DELETE", "HEAD"))
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

    obj <- list()
    obj$endpoint <- endpoint
    obj$path <- path
    obj$options <- options
    obj$headers <- utils::modifyList(headers, list(Accept=accept, DataServiceVersion="3.0;NetFx"))
    obj$method <- match.arg(http_verb)
    obj$body <- body
    structure(obj, class="batch_operation")
}


serialize_batch_operation <- function(object)
{
    UseMethod("serialize_batch_operation")
}


serialize_batch_operation.batch_operation <- function(object)
{
        url <- httr::parse_url(object$endpoint$url)
        url$path <- object$path
        url$query <- object$options

        preamble <- c(
            "Content-Type: application/http",
            "Content-Transfer-Encoding: binary",
            "",
            paste(object$method, httr::build_url(url), "HTTP/1.1"),
            paste0(names(object$headers), ": ", object$headers),
            if(!is.null(object$body)) "Content-Type: application/json"
        )

        if(is.null(object$body))
            preamble
        else if(!is.character(object$body))
        {
            body <- jsonlite::toJSON(object$body, auto_unbox=TRUE, null="null")
            # special-case treatment for 1-row dataframes
            if(is.data.frame(object$body) && nrow(object$body) == 1)
                body <- substr(body, 2, nchar(body) - 1)
            c(preamble, "", body)
        }
        else c(preamble, "", object$body)
}


#' @rdname table_batch
#' @export
do_batch_transaction <- function(endpoint, operations, batch_status_handler=c("warn", "stop", "message", "pass"))
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
    serialized <- lapply(operations, function(op) c(paste0("--", changeset_bound), serialize_batch_operation(op)))
    body <- paste0(c(batch_preamble, unlist(serialized), batch_postscript), collapse="\n")
    if(nchar(body) > 4194304)
        stop("Batch request too large, must be 4MB or less")

    res <- call_table_endpoint(endpoint, "$batch", headers=headers, body=body, encode="raw",
        http_verb="POST")
    process_batch_response(res, match.arg(batch_status_handler))
}


process_batch_response <- function(response, batch_status_handler)
{
    # assume response (including body) is always text
    response <- rawToChar(response)
    lines <- strsplit(response, "\r?\n\r?")[[1]]
    batch_bound <- lines[1]
    changeset_bound <- sub("^.+boundary=(.+)$", "\\1", lines[2])
    n <- length(lines)

    # assume only 1 changeset
    batch_end <- grepl(batch_bound, lines[n])
    if(!any(batch_end))
        stop("Invalid batch response, batch boundary not found", call.=FALSE)
    changeset_end <- grepl(changeset_bound, lines[n-1])
    if(!any(changeset_end))
        stop("Invalid batch response, changeset boundary not found", call.=FALSE)

    lines <- lines[3:(n-3)]
    op_bounds <- grep(changeset_bound, lines)
    op_responses <- Map(
        function(start, end) process_operation_response(lines[seq(start, end)], batch_status_handler),
        op_bounds + 1,
        c(op_bounds[-1], length(lines))
    )
    op_responses
}


process_operation_response <- function(response, handler)
{
    blanks <- which(response == "")
    if(length(blanks) < 2)
        stop("Invalid operation response", call.=FALSE)

    headers <- response[seq(blanks[1]+1, blanks[2]-1)]  # skip over http stuff

    status <- as.numeric(sub("^.+ (\\d{3}) .+$", "\\1", headers[1]))
    headers <- strsplit(headers[-1], ": ")
    names(headers) <- sapply(headers, `[[`, 1)
    headers <- sapply(headers, `[[`, 2, simplify=FALSE)
    class(headers) <- c("insensitive", "list")

    if(status >= 300)
    {
        if(handler == "stop")
            stop(httr::http_condition(status, "error"))
        else if(handler == "warn")
            warning(httr::http_condition(status, "warning"))
        else if(handler == "message")
            message(httr::http_condition(status, "message"))
    }

    body <- if(!(status %in% c(204, 205)) && blanks[2] < length(response))
        response[seq(blanks[2]+1, length(response))]
    else NULL

    obj <- list(status=status, headers=headers, body=body)
    class(obj) <- "batch_operation_response"
    obj
}


#' @export
print.batch_operation <- function(x, ...)
{
    cat("<Table storage batch operation>\n")
    invisible(x)
}

#' @export
print.batch_operation_response <- function(x, ...)
{
    cat("<Table storage batch operation response>\n")
    invisible(x)
}

