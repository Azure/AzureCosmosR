#' Container partition key information
#'
#' @param container An object of class `cosmos_container`.
#' @details
#' These are functions to facilitate working with a Cosmos DB container, which often requires knowledge of its partition key.
#' @return
#' For `get_partition_key`, the name of the partition key column as a string.
#'
#' For `list_partition_key_values`, a character vector of all the values of the partition key.
#'
#' For `list_partition_key_ranges`, a character vector of the IDs of the partition key ranges.
#' @rdname partition_key
#' @export
get_partition_key <- function(container)
{
    key <- container$partitionKey$paths
    if(is.character(key))
        sub("^/", "", key)
    else NULL
}


#' @rdname partition_key
#' @export
list_partition_key_values <- function(container)
{
    key <- get_partition_key(container)[1]
    qry <- sprintf("select distinct value %s.%s from %s", container$id, key, container$id)
    lst <- suppressMessages(query_documents(container, qry, by_pkrange=TRUE))
    unique(unlist(lst))
}


#' @rdname partition_key
#' @export
list_partition_key_ranges <- function(container)
{
    res <- do_cosmos_op(container, "pkranges", "pkranges", "",
        headers=list(`x-ms-documentdb-query-enablecrosspartition`=TRUE))
    lst <- process_cosmos_response(res)
    sapply(lst$PartitionKeyRanges, `[[`, "id")
}

