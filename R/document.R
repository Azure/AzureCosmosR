#' @export
get_document <- function(container, ...)
{
    UseMethod("get_document")
}

#' @export
get_document.cosmos_container <- function(container, id, partition_key, metadata=TRUE, headers=list(), ...)
{
    headers <- utils::modifyList(headers, list(`Content-Type`="application/query+json"))
    headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(partition_key)

    res <- do_cosmos_op(container, file.path("docs", id), "docs", file.path("docs", id), headers=headers, ...)
    doc <- process_cosmos_response(res, simplify=FALSE)
    if(!metadata)
        doc[c("id", "_rid", "_self", "_etag", "_attachments", "_ts")] <- NULL
    doc
}


#' @export
create_document <- function(container, ...)
{
    UseMethod("create_document")
}

#' @export
create_document.cosmos_container <- function(container, properties, headers=list(), ...)
{
    if(is.character(properties) && jsonlite::validate(properties))
        properties <- jsonlite::fromJSON(properties)

    # assume only 1 partition key at most
    if(!is.null(container$partitionKey))
    {
        partition_key <- sub("^/", "", container$partitionKey$paths)
        if(is.null(properties[[partition_key]]))
            stop("Partition key not found in document properties", call.=FALSE)
        headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(properties[[partition_key]])
    }
    if(is.null(properties$id))
        properties$id <- uuid::UUIDgenerate()
    properties <- jsonlite::toJSON(properties, auto_unbox=TRUE, null="null")

    res <- do_cosmos_op(container, "docs", "docs", "", headers=headers, body=properties, http_verb="POST", ...)
    invisible(process_cosmos_response(res))
}


#' @export
list_documents <- function(container, ...)
{
    UseMethod("list_documents")
}

#' @export
list_documents.cosmos_container <- function(container, partition_key=NULL, as_data_frame=FALSE, metadata=FALSE,
    headers=list(), ...)
{
    headers <- utils::modifyList(headers, list(`Content-Type`="application/query+json"))
    if(!is.null(partition_key))
        headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(partition_key)

    res <- do_cosmos_op(container, "docs", "docs", headers=headers, ...)
    if(inherits(res, "response"))
        get_docs(res, as_data_frame, metadata, ...)
    else do.call(vctrs::vec_rbind, lapply(res, get_docs, as_data_frame=as_data_frame, metadata=metadata, ...))
}


#' @export
delete_document <- function(container, ...)
{
    UseMethod("delete_document")
}

#' @export
delete_document.cosmos_container <- function(container, id, partition_key, headers=list(), confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, id, "document"))
        return(invisible(NULL))

    path <- file.path("docs", id)
    headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(partition_key)
    res <- do_cosmos_op(container, path, "docs", path, headers=headers, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res, ...))
}

