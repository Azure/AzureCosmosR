#' Methods for working with Azure Cosmos DB attachments
#'
#' @param document A Cosmos DB document object, as obtained by `get_document`, `create_document`, `list_documents` or `query_documents`.
#' @param file For `create_attachment`, the file to turn into an attachment. This can be a filename, a raw connection, or a URL connection pointing to a location on the Internet.
#' @param content_type For `create_attachment`, the content type of the attachment. Defaults to "application/octet-stream".
#' @param id For `create_attachment` and `delete_attachment`, the ID for the attachment. This is optional for `create_attachment`.
#' @param attachment For `download_attachment`, the attachment object, as obtained by `list_attachments` or `create_attachment`.
#' @param destfile For `download_attachment`, the destination filename.
#' @param overwrite For `download_attachment`, whether to overwrite an existing destination file.
#' @param confirm For `delete_attachment`, whether to ask for confirmation before deleting.
#' @param options,headers,... Optional arguments passed to lower-level functions.
#' @details
#' These are methods for working with document attachments in Cosmos DB using the core (SQL) API. Note that the attachments API is deprecated; going forward, you should explore other options for hosting external files, such as Azure blob storage.
#'
#' @aliases cosmos_attachment
#' @rdname cosmos_attachment
#' @export
list_attachments <- function(document, ...)
{
    UseMethod("list_attachments")
}

#' @rdname cosmos_attachment
#' @export
list_attachments.cosmos_document <- function(document, ...)
{
    res <- do_cosmos_op(document, "attachments", "attachments", "", ...)
    atts <- if(inherits(res, "response"))
        process_cosmos_response(res, ...)$Attachments
    else unlist(lapply(process_cosmos_response(res, ...), `[[`, "Attachments"), recursive=FALSE)
    lapply(atts, as_attachment, document=document)
}


#' @rdname cosmos_attachment
#' @export
create_attachment <- function(document, ...)
{
    UseMethod("create_attachment")
}

#' @rdname cosmos_attachment
#' @export
create_attachment.cosmos_document <- function(document, file, content_type="application/octet-stream",
    id=NULL, headers=list(), ...)
{
    # read the data if a (non-URL) connection or filename
    if((inherits(file, "connection") && !inherits(file, "url")) ||
       (is.character(file) && length(file) == 1 && file.exists(file)))
    {
        if(is.character(file))
            file <- file(file, open="rb")

        on.exit(close(file))
        body <- raw(0)
        repeat
        {
            chunk <- readBin(file, "raw", 1e6)
            if(length(chunk) == 0)
                break
            body <- c(body, chunk)
        }
        headers$`Content-Type` <- content_type
    }
    else
    {
        if(inherits(file, "url"))
            file <- summary(file)$description
        if(is.null(id))
            id <- uuid::UUIDgenerate()
        body <- jsonlite::toJSON(list(
            id=id,
            contentType=content_type,
            media=file
        ), auto_unbox=TRUE)
    }
    res <- do_cosmos_op(document, "attachments", "attachments", "", headers=headers, body=body, http_verb="POST", ...)
    att <- process_cosmos_response(res, ...)
    invisible(as_attachment(att, document))
}


#' @rdname cosmos_attachment
#' @export
download_attachment <- function(attachment, ...)
{
    UseMethod("download_attachment")
}

#' @rdname cosmos_attachment
#' @export
download_attachment.cosmos_attachment <- function(attachment, destfile, options=list(), headers=list(),
    overwrite=FALSE, ...)
{
    url <- httr::parse_url(attachment$media)
    if(is.null(url$scheme))  # attachment is hosted in Cosmos DB
    {
        key <- attachment$document$container$database$endpoint$key
        reslink <- tolower(attachment$`_rid`)
        now <- httr::http_date(Sys.time())
        sig <- sign_cosmos_request(key, "GET", "media", reslink, now)
        headers <- utils::modifyList(headers, list(
            Authorization=sig,
            `x-ms-date`=now,
            `x-ms-version`=attachment$document$container$database$endpoint$api_version
        ))
        url <- attachment$document$container$database$endpoint$host
        url$path <- attachment$media
    }
    else url$query <- options

    httr::GET(url, do.call(httr::add_headers, headers),
              config=httr::write_disk(destfile, overwrite=overwrite), httr::progress())
}


#' @rdname cosmos_attachment
#' @export
delete_attachment <- function(document, ...)
{
    UseMethod("delete_attachment")
}

#' @rdname cosmos_attachment
#' @export
delete_attachment.cosmos_document <- function(document, id, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, id, "attachment"))
        return(invisible(NULL))

    path <- file.path("attachments", id)
    res <- do_cosmos_op(document, path, "attachments", path, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res, ...))
}

#' @rdname cosmos_attachment
#' @export
delete_attachment.cosmos_attachment <- function(document, ...)
{
    delete_attachment(document$document, document$id, ...)
}


#' @export
print.cosmos_attachment <- function(x, ...)
{
    cat("Cosmos DB document attachment '", x$id, "'\n", sep="")
    invisible(x)
}


as_attachment <- function(attachment, document)
{
    attachment$document <- document
    structure(attachment, class="cosmos_attachment")
}
