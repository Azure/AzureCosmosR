#' @export
insert_table_entity <- function(table, entity)
{
    if(is.character(entity) && jsonlite::validate(entity))
        entity <- jsonlite::fromJSON(entity, simplifyDataFrame=FALSE)
    else if(is.data.frame(entity))
    {
        if(nrow(entity) == 1) # special-case treatment for 1-row dataframes
            entity <- unclass(entity)
        else stop("Can only insert one row at a time; use import_table_entities() to insert multiple rows")
    }

    check_column_names(entity)
    headers <- list(Prefer="return-no-content")
    res <- call_table_endpoint(table$endpoint, table$name, body=entity, headers=headers, http_verb="POST",
                               http_status_handler="pass")
    httr::stop_for_status(res, storage_error_message(res))
    invisible(httr::headers(res)$ETag)
}


#' @export
delete_table_entity <- function(table, partition_key, row_key, etag=NULL)
{
    path <- sprintf("%s(PartitionKey='%s',RowKey='%s')", table$name, partition_key, row_key)
    if(is.null(etag))
        etag <- "*"
    headers <- list(`If-Match`=etag)
    invisible(call_table_endpoint(table$endpoint, path, headers=headers, http_verb="DELETE"))
}


#' @export
list_table_entities <- function(table, filter=NULL, select=NULL, as_data_frame=TRUE)
{
    path <- sprintf("%s()", table$name)
    opts <- list(
        `$filter`=filter,
        `$select`=paste0(select, collapse=",")
    )
    val <- list()
    repeat
    {
        res <- call_table_endpoint(table$endpoint, path, options=opts, http_status_handler="pass")
        heads <- httr::headers(res)
        res <- httr::content(res)
        val <- c(val, res$value)

        if(is.null(heads$`x-ms-continuation-NextPartitionKey`))
            break
        opts$NextPartitionKey <- heads$`x-ms-continuation-NextPartitionKey`
        opts$NextRowKey <- heads$`x-ms-continuation-NextRowKey`
    }

    # table storage allows columns to vary by row, so cannot use base::rbind
    if(as_data_frame)
        do.call(vctrs::vec_rbind, val)
    else val
}


#' @export
get_table_entity <- function(table, partition_key, row_key, select=NULL)
{
    path <- sprintf("%s(PartitionKey='%s',RowKey='%s')", table$name, partition_key, row_key)
    opts <- list(`$select`=paste0(select, collapse=","))
    call_table_endpoint(table$endpoint, path, options=opts)
}


#' @export
import_table_entities <- function(table, data, partition_key=NULL, row_key=NULL)
{
    if(is.character(data) && jsonlite::validate(data))
        data <- jsonlite::fromJSON(data, simplifyDataFrame=TRUE)

    if(!is.null(partition_key))
        names(data)[names(data) == partition_key] <- "PartitionKey"
    if(!is.null(row_key))
        names(data)[names(data) == row_key] <- "RowKey"

    check_column_names(data)
    endpoint <- table$endpoint
    path <- table$name
    headers <- list(Prefer="return-no-content")
    res <- lapply(split(data, data$PartitionKey), function(dfpart)
    {
        n <- nrow(dfpart)
        nchunks <- n %/% 100 + (n %% 100 > 0)
        lapply(seq_len(nchunks), function(chunk)
        {
            rows <- seq(from=(chunk-1)*100 + 1, to=min(chunk*100, n))
            dfchunk <- dfpart[rows, ]
            ops <- lapply(seq_len(nrow(dfchunk)), function(i)
                create_batch_operation(endpoint, path, body=dfchunk[i, ], headers=headers, http_verb="POST"))
            send_batch_request(endpoint, ops)
        })
    })
    invisible(unlist(res))
}


check_column_names <- function(data)
{
    if(!("PartitionKey" %in% names(data)) || !("RowKey" %in% names(data)))
        stop("Data must contain columns named 'PartitionKey' and 'RowKey'", call.=FALSE)
    if(!(is.character(data$PartitionKey) || is.factor(data$PartitionKey)) ||
       !(is.character(data$RowKey) || is.factor(data$RowKey)))
        stop("RowKey and PartitionKey columns must be character or factor", call.=FALSE)
    if("Timestamp" %in% names(data))
        stop("'Timestamp' column is reserved for system use", call.=FALSE)
}
