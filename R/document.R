#' Methods for working with Azure Cosmos DB documents
#'
#' @param object A Cosmos DB container object, as obtained by `get_cosmos_container` or `create_cosmos_container`.
#' @param id The document ID.
#' @param partition_key For `get_document` and `delete_document`, the value of the partition key for the desired document. For `list_documents`, restrict the returned list only to documents with this key value.
#' @param data For `create_document`, the document data. This can be either a string containing JSON text, or a (possibly nested) list containing the parsed JSON.
#' @param metadata For `get_document` and `list_documents`, whether to include Cosmos DB document metadata in the result.
#' @param as_data_frame For `list_documents`, whether to return a data frame or a list of Cosmos DB document objects. Note that the default value is FALSE, unlike [query_documents].
#' @param confirm For `delete_cosmos_container`, whether to ask for confirmation before deleting.
#' @param headers,... Optional arguments passed to lower-level functions.
#' @details
#' These are low-level functions for working with individual documents in a Cosmos DB container. In most cases you will want to use [query_documents] to issue queries against the container, or [bulk_import] and [bulk_delete] to create and delete documents.
#' @return
#' `get_document` and `create_document` return an object of S3 class `cosmos_document`. The actual document contents can be found in the `data` component of this object.
#'
#' `list_documents` returns a list of `cosmos_document` objects if `as_data_frame` is FALSE, and a data frame otherwise.
#' @seealso
#' [query_documents], [bulk_import], [bulk_delete], [cosmos_container]
#' @examples
#' \dontrun{
#'
#' endp <- cosmos_endpoint("https://myaccount.documents.azure.com:443/", key="mykey")
#' db <- get_cosmos_database(endp, "mydatabase")
#'
#' cont <- get_cosmos_container(db, "mycontainer")
#'
#' # a list of document objects
#' list_documents(cont)
#'
#' # a data frame
#' list_documents(cont, as_data_frame=TRUE)
#'
#' # a single document
#' doc <- get_document(cont, "mydocumentid")
#' doc$data
#'
#' delete_document(doc)
#'
#' }
#' @aliases cosmos_document
#' @rdname cosmos_document
#' @export
get_document <- function(object, ...)
{
    UseMethod("get_document")
}

#' @export
get_document.cosmos_container <- function(object, id, partition_key, metadata=TRUE, headers=list(), ...)
{
    headers <- utils::modifyList(headers, list(`Content-Type`="application/query+json"))
    headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(partition_key)

    res <- do_cosmos_op(object, file.path("docs", id), "docs", file.path("docs", id), headers=headers, ...)
    doc <- process_cosmos_response(res, simplify=FALSE)
    if(!metadata)
        doc[c("id", "_rid", "_self", "_etag", "_attachments", "_ts")] <- NULL
    as_document(doc, object)
}


#' @rdname cosmos_document
#' @export
create_document <- function(object, ...)
{
    UseMethod("create_document")
}

#' @rdname cosmos_document
#' @export
create_document.cosmos_container <- function(object, data, headers=list(), ...)
{
    if(is.character(data) && jsonlite::validate(data))
        data <- jsonlite::fromJSON(data)

    # assume only 1 partition key at most
    if(!is.null(object$partitionKey))
    {
        partition_key <- sub("^/", "", object$partitionKey$paths)
        if(is.null(data[[partition_key]]))
            stop("Partition key not found in document data", call.=FALSE)
        headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(data[[partition_key]])
    }
    if(is.null(data$id))
        data$id <- uuid::UUIDgenerate()
    data <- jsonlite::toJSON(data, auto_unbox=TRUE, null="null")

    res <- do_cosmos_op(object, "docs", "docs", "", headers=headers, body=data, http_verb="POST", ...)
    doc <- process_cosmos_response(res)
    invisible(as_document(doc, object))
}


#' @rdname cosmos_document
#' @export
list_documents <- function(object, ...)
{
    UseMethod("list_documents")
}

#' @rdname cosmos_document
#' @export
list_documents.cosmos_container <- function(object, partition_key=NULL, as_data_frame=FALSE, metadata=TRUE,
    headers=list(), ...)
{
    headers <- utils::modifyList(headers, list(`Content-Type`="application/query+json"))
    if(!is.null(partition_key))
        headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(partition_key)

    res <- do_cosmos_op(object, "docs", "docs", headers=headers, ...)
    get_docs(res, as_data_frame, metadata, object)
}


#' @rdname cosmos_document
#' @export
delete_document <- function(object, ...)
{
    UseMethod("delete_document")
}

#' @rdname cosmos_document
#' @export
delete_document.cosmos_container <- function(object, id, partition_key, headers=list(), confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, id, "document"))
        return(invisible(NULL))

    path <- file.path("docs", id)
    headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(partition_key)
    res <- do_cosmos_op(object, path, "docs", path, headers=headers, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res))
}

#' @rdname cosmos_document
#' @export
delete_document.cosmos_document <- function(object, ...)
{
    partition_key <- sub("^/", "", object$container$partitionKey$paths)
    delete_document(object$container, object$data$id, object$data[[partition_key]], ...)
}


#' @export
print.cosmos_document <- function(x, ...)
{
    cat("Cosmos DB document '", x$data$id, "'\n", sep="")
    invisible(x)
}


as_document <- function(document, container)
{
    structure(list(container=container, data=document), class="cosmos_document")
}
