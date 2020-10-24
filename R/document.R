#' @export
get_document <- function(container, id, partition_key=NULL, metadata=TRUE, headers=list(), ...)
{
    headers <- utils::modifyList(headers, list(`Content-Type`="application/query+json"))
    if(!is.null(partition_key))
        headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(partition_key)

    res <- do_cosmos_op(container, file.path("docs", id), "docs", file.path("docs", id), headers=headers, ...)
    doc <- process_cosmos_response(res, simplify=FALSE)
    if(!metadata)
        doc[c("id", "_rid", "_self", "_etag", "_attachments", "_ts")] <- NULL
    doc
}


