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
        process_cosmos_response(res, ...)$Attachments
    else lapply(process_cosmos_response(res, ...), `[[`, "Attachments")
    atts
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
    process_cosmos_response(res, ...)
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
    process_cosmos_response(res, ...)
}
