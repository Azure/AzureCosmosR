#' @export
run_query <- function(container, query, parameters=list(),
    cross_partition=TRUE, partition_key=NULL, as_data_frame=TRUE, metadata=FALSE)
{
    headers <- list(`Content-Type`="application/query+json")
    if(cross_partition)
        headers$`x-ms-documentdb-query-enablecrosspartition` <- TRUE
    if(!is.null(partition_key))
        headers$`x-ms-documentdb-partitionkey` <- partition_key

    body <- list(query=query, parameters=make_parameter_list(parameters))

    res <- do_cosmos_op(container, "docs", resource_type="docs", headers=headers, body=body, encode="json",
                        http_verb="POST")
    lst <- process_cosmos_response(res, simplify=as_data_frame)$Documents
    if(is.data.frame(lst) && !metadata)
        lst[c("id", "_rid", "_self", "_etag", "_attachments", "_ts")] <- NULL
    lst
}


make_parameter_list <- function(parlist)
{
    nams <- names(parlist)
    noatsign <- !grepl("^@", nams)
    nams[noatsign] <- paste0("@", nams[noatsign])
    Map(function(n, v) c(name=n, value=v), nams, parlist, USE.NAMES=FALSE)
}

