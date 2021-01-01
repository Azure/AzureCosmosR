#' Query an Azure Cosmos DB container
#'
#' @param container A Cosmos DB container object, as obtained by `get_cosmos_container` or `create_cosmos_container`.
#' @param query A string containing the query text.
#' @param parameters A named list of parameters to pass to a parameterised query, if required.
#' @param cross_partition,partition_key,by_physical_partition Arguments that control how to handle cross-partition queries. See 'Details' below.
#' @param as_data_frame Whether to return the query result as a data frame, or a list of Cosmos DB document objects.
#' @param metadata Whether to include Cosmos DB document metadata in the query result.
#' @param headers,... Optional arguments passed to lower-level functions.
#' @details
#' This is the primary function for querying the contents of a Cosmos DB container (table). The `query` argument should contain the text of a SQL query, optionally parameterised. if the query contains parameters, pass them in the `parameters` argument as a named list.
#'
#' Cosmos DB is a partitioned key-value store under the hood, with documents stored in separate physical databases according to their value of the partition key. AzureCosmosR has limited support for cross-partition queries: basic SELECTs should work, but aggregates and more complex queries may require some hand-hacking.
#'
#' The default `cross_partition=TRUE` runs the query for all partition key values and then attempts to stitch the results together. To run the query for only one key value, set `cross_partition=FALSE` and `partition_key` to the desired value. You can obtain all the values of the key with the [list_key_values] function.
#'
#' The `by_physical_partition` argument allows running the query separately across all _physical_ partitions. A physical partition contains the documents for one or more key values. You can set this to TRUE to run a query that fails when run across partitions; the returned object will be a list containing the individual query results from each physical partition.
#'
#' As an alternative to AzureCosmosR, you can also use the ODBC protocol to interface with the SQL API. By instaling a suitable ODBC driver, you can then talk to Cosmos DB in a manner similar to any SQL database. An advantage of the ODBC interface is that it fully supports cross-partition queries, which AzureCosmosR currently only partially supports. A disadvantage is that it does not support nested document fields; such fields will be flattened into a string.
#' @seealso
#' [cosmos_container], [cosmos_document], [list_key_values]
#' @export
query_documents <- function(container, ...)
{
    UseMethod("query_documents")
}

#' @rdname query_documents
#' @export
query_documents.cosmos_container <- function(container, query, parameters=list(),
    cross_partition=TRUE, partition_key=NULL, by_physical_partition=FALSE,
    as_data_frame=TRUE, metadata=TRUE, headers=list(), ...)
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

    # sending query to individual partitions (low-level API)
    if(by_physical_partition)
    {
        message("Running query on individual physical partitions")
        # if(query_needs_rewrite(res))
        # {
        #     message("Also rewriting query for individual physical partitions")
        #     body$query <- rewrite_query(res)
        # }
        part_ids <- get_partition_physical_ids(container)
        lapply(part_ids, function(id)
        {
            headers$`x-ms-documentdb-partitionkeyrangeid` <- id
            res_part <- do_cosmos_op(container, "docs", "docs", headers=headers, body=body, encode="json",
                                     http_verb="POST", ...)
            get_docs(res_part, as_data_frame, metadata, container)
        })
    }
    else get_docs(res, as_data_frame, metadata, container)
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
    docs <- process_cosmos_response(response, simplify=as_data_frame)

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


# bad_query_with_valid_syntax <- function(response)
# {
#     if(!inherits(response, "response") || httr::status_code(response) != 400)
#         return(FALSE)
#     cont <- httr::content(response)
#     is.character(cont$message) && !grepl("Syntax error", cont$message, fixed=TRUE)
# }


# query_needs_rewrite <- function(response)
# {
#     cont <- httr::content(response)
#     if(is.null(cont$additionalErrorInfo))
#         return(FALSE)

#     qry <- try(jsonlite::fromJSON(cont$additionalErrorInfo)$queryInfo$rewrittenQuery, silent=TRUE)
#     if(inherits(qry, "try-error"))
#         return(FALSE)

#     is.character(qry) && nchar(qry) > 0
# }


# rewrite_query <- function(response)
# {
#     cont <- httr::content(response)
#     jsonlite::fromJSON(cont$additionalErrorInfo)$queryInfo$rewrittenQuery
# }
