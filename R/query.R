#' @export
query_documents <- function(container, ...)
{
    UseMethod("query_documents")
}

#' @export
query_documents.cosmos_container <- function(container, query, parameters=list(),
    cross_partition=TRUE, partition_key=NULL,
    as_data_frame=TRUE, metadata=FALSE, headers=list(), ...)
{
    headers <- utils::modifyList(headers, list(`Content-Type`="application/query+json"))
    if(cross_partition)
        headers$`x-ms-documentdb-query-enablecrosspartition` <- TRUE
    if(!is.null(partition_key))
        headers$`x-ms-documentdb-partitionkey` <- jsonlite::toJSON(partition_key)

    if(length(query) > 1)
        query <- paste0(query, collapse="\n")
    body <- list(query=query, parameters=make_parameter_list(parameters))
    res <- do_cosmos_op(container, "docs", "docs", headers=headers, body=body, encode="json", http_verb="POST", ...)
    get_docs(res, as_data_frame, metadata, container)
}


make_parameter_list <- function(parlist)
{
    parnames <- names(parlist)
    noatsign <- substr(parnames, 1, 1) != "@"
    parnames[noatsign] <- paste0("@", parnames[noatsign])
    Map(function(n, v) c(name=n, value=v), parnames, parlist, USE.NAMES=FALSE)
}


get_docs <- function(response, as_data_frame, metadata, container)
{
    docs <- process_cosmos_response(response, simplifyVector=TRUE, simplifyDataFrame=as_data_frame)

    if(as_data_frame)
    {
        docs <- if(inherits(response, "response"))
            docs$Documents
        else do.call(vctrs::vec_rbind, lapply(docs, `[[`, "Documents"))

        if(AzureRMR::is_empty(docs))
            return(data.frame())

        if(!metadata && is.data.frame(docs))  # a query can return scalars rather than documents
            docs[c("id", "_rid", "_self", "_etag", "_attachments", "_ts")] <- NULL
        return(docs)
    }
    else
    {
        docs <- if(inherits(response, "response"))
            docs$Documents
        else unlist(lapply(docs, `[[`, "Documents"), recursive=FALSE, use.names=FALSE)
        return(lapply(docs, as_document, container=container))
    }
}
