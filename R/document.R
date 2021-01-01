#' Methods for working with Azure Cosmos DB documents
#'
#' @param container A Cosmos DB container object, as obtained by `get_cosmos_container` or `create_cosmos_container`.
#' @param id The document ID.
#' @param partition_key For `get_document` and `delete_document`, the value of the partition key for the desired document. For `list_documents`, restrict the returned list only to documents with this key value.
#' @param data For `create_document`, the document data. This can be either a string containing JSON text, or a (possibly nested) list containing the parsed JSON.
#' @param metadata For `get_document` and `list_documents`, whether to include Cosmos DB document metadata in the result.
#' @param as_data_frame For `list_documents`, whether to return a data frame or a list of Cosmos DB document objects.
#' @param confirm For `delete_cosmos_container`, whether to ask for confirmation before deleting.
#' @param headers,... Optional arguments passed to lower-level functions.
#' @details
#' These are low-level functions for working with individual documents in a Cosmos DB container. In most cases you will want to use [query_documents] to issue queries against the container, or [bulk_import] and [bulk_delete] to create and delete documents.
#' @seealso
#' [query_documents], [bulk_import], [bulk_delete], [cosmos_container]
#' @aliases cosmos_document
#' @rdname cosmos_document
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
    as_document(doc, container)
}


#' @rdname cosmos_document
#' @export
create_document <- function(container, ...)
{
    UseMethod("create_document")
}

#' @rdname cosmos_document
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
    invisible(as_document(obj, container))
}


#' @rdname cosmos_document
#' @export
list_documents <- function(container, ...)
{
    UseMethod("list_documents")
}

#' @rdname cosmos_document
#' @export
list_documents.cosmos_container <- function(container, partition_key=NULL, as_data_frame=FALSE, metadata=TRUE,
    headers=list(), ...)
{
    headers <- utils::modifyList(headers, list(`Content-Type`="application/query+json"))
    if(!is.null(partition_key))
        headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(partition_key)

    res <- do_cosmos_op(container, "docs", "docs", headers=headers, ...)
    get_docs(res, as_data_frame, metadata, container)
}


#' @rdname cosmos_document
#' @export
delete_document <- function(container, ...)
{
    UseMethod("delete_document")
}

#' @rdname cosmos_document
#' @export
delete_document.cosmos_container <- function(container, id, partition_key, headers=list(), confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, id, "document"))
        return(invisible(NULL))

    path <- file.path("docs", id)
    headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(partition_key)
    res <- do_cosmos_op(container, path, "docs", path, headers=headers, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res))
}

#' @rdname cosmos_document
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


#' @rdname do_cosmos_op
#' @export
do_cosmos_op.cosmos_document <- function(object, path="", resource_type="docs", resource_link="", headers=list(), ...)
{
    full_path <- full_reslink <-  file.path("dbs", object$container$database$id,
        "colls", object$container$id, "docs", object$data$id)
    if(nchar(path) > 0)
        full_path <- file.path(full_path, path)
    if(nchar(resource_link) > 0)
        full_reslink <- file.path(full_reslink, resource_link)

    partition_key <- sub("^/", "", object$container$partitionKey$paths)
    headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(object$data[[partition_key]])
    call_cosmos_endpoint(object$container$database$endpoint, full_path, resource_type, full_reslink,
                         headers=headers, ...)
}


as_document <- function(document, container)
{
    structure(list(container=container, data=document), class="cosmos_document")
}
