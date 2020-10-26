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
    structure(list(container=container, data=doc), class="cosmos_document")
}


#' @export
create_document <- function(container, ...)
{
    UseMethod("create_document")
}

#' @export
create_document.cosmos_container <- function(container, data, headers=list(), ...)
{
    if(is.character(data) && jsonlite::validate(data))
        data <- jsonlite::fromJSON(data)

    # assume only 1 partition key at most
    if(!is.null(container$partitionKey))
    {
        partition_key <- sub("^/", "", container$partitionKey$paths)
        if(is.null(data[[partition_key]]))
            stop("Partition key not found in document data", call.=FALSE)
        headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(data[[partition_key]])
    }
    if(is.null(data$id))
        data$id <- uuid::UUIDgenerate()
    data <- jsonlite::toJSON(data, auto_unbox=TRUE, null="null")

    res <- do_cosmos_op(container, "docs", "docs", "", headers=headers, body=data, http_verb="POST", ...)
    obj <- process_cosmos_response(res)
    invisible(structure(list(container=container, data=obj), class="cosmos_document"))
}


#' @export
list_documents <- function(container, ...)
{
    UseMethod("list_documents")
}

#' @export
list_documents.cosmos_container <- function(container, partition_key=NULL, as_data_frame=FALSE, metadata=TRUE,
    headers=list(), ...)
{
    headers <- utils::modifyList(headers, list(`Content-Type`="application/query+json"))
    if(!is.null(partition_key))
        headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(partition_key)

    res <- do_cosmos_op(container, "docs", "docs", headers=headers, ...)
    get_docs(res, as_data_frame, metadata, container, ...)
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

#' @export
delete_document.cosmos_document <- function(container, ...)
{
    partition_key <- sub("^/", "", container$container$partitionKey$paths)
    delete_document(container$container, container$data$id, container$data[[partition_key]], ...)
}


#' @export
print.cosmos_document <- function(x, ...)
{
    cat("Cosmos DB document '", x$data$id, "'\n", sep="")
    invisible(x)
}
