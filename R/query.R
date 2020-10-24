#' @export
query_documents <- function(container, query, parameters=list(), cross_partition=TRUE, partition_key=NULL,
    as_data_frame=TRUE, metadata=FALSE, headers=list(), ...)
{
    headers <- utils::modifyList(headers, list(`Content-Type`="application/query+json"))
    if(cross_partition)
        headers$`x-ms-documentdb-query-enablecrosspartition` <- TRUE
    if(!is.null(partition_key))
        headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(partition_key)

    body <- list(query=query, parameters=make_parameter_list(parameters))
    res <- do_cosmos_op(container, "docs", "docs", headers=headers, body=body, encode="json", http_verb="POST", ...)

    if(inherits(res, "response"))
        get_docs(res, as_data_frame, metadata, ...)
    else do.call(vctrs::vec_rbind, lapply(res, get_docs, as_data_frame=as_data_frame, metadata=metadata, ...))
}


make_parameter_list <- function(parlist)
{
    nams <- names(parlist)
    noatsign <- !grepl("^@", nams)
    nams[noatsign] <- paste0("@", nams[noatsign])
    Map(function(n, v) c(name=n, value=v), nams, parlist, USE.NAMES=FALSE)
}


get_docs <- function(response, as_data_frame, metadata, ...)
{
    docs <- process_cosmos_response(response, simplify=as_data_frame, ...)$Documents
    if(as_data_frame && AzureRMR::is_empty(docs))
        return(data.frame())
    if(as_data_frame && !metadata)
        docs[c("id", "_rid", "_self", "_etag", "_attachments", "_ts")] <- NULL
    docs
}
