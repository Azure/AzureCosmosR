#' @export
list_attachments <- function(document, ...)
{
    UseMethod("list_attachments")
}

#' @export
list_attachments.cosmos_document <- function(document, ...)
{
    path <- "attachments"
    res <- do_cosmos_op(document, path, "attachments", "", ...)
    atts <- if(inherits(res, "response"))
        process_cosmos_response(res)$Attachments
    else lapply(process_cosmos_response(res), `[[`, "Attachments")
    lapply(atts, as_attachment, document=document)
}


#' @export
create_attachment <- function(document, ...)
{
    UseMethod("create_attachment")
}

#' @export
create_attachment.cosmos_document <- function(document, file, content_type, id=NULL, headers=list(), ...)
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


#' @export
download_attachment <- function(attachment, ...)
{
    UseMethod("download_attachment")
}

#' @export
download_attachment.cosmos_attachment <- function(attachment, destfile, overwrite=FALSE, ...)
{
    key <- attachment$document$container$database$endpoint$key
    reslink <- tolower(attachment$`_rid`)
    now <- Sys.time()
    sig <- sign_cosmos_request(key, "GET", "media", reslink, now)
    headers <- list(
        Authorization=sig,
        `x-ms-date`=httr::http_date(now),
        `x-ms-version`=attachment$document$container$database$endpoint$api_version
    )
    url <- attachment$document$container$database$endpoint$host
    url$path <- attachment$media

    httr::GET(url, do.call(httr::add_headers, headers),
              config=httr::write_disk(destfile, overwrite=overwrite), httr::progress())
}


#' @export
delete_attachment <- function(document, ...)
{
    UseMethod("delete_attachment")
}

#' @export
delete_attachment.cosmos_document <- function(document, id, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, id, "attachment"))
        return(invisible(NULL))

    path <- file.path("attachments", id)
    res <- do_cosmos_op(document, path, "attachments", path, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res, ...))
}

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
