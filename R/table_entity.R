#' @export
update_table_entity <- function(table, entity, partition_key=entity$PartitionKey, row_key=entity$RowKey, etag=NULL)
{
    if(is.character(entity) && jsonlite::validate(entity))
        entity <- jsonlite::fromJSON(entity, simplifyDataFrame=FALSE)

    path <- sprintf("%s(PartitionKey='%s',RowKey='%s')", table$name, partition_key, row_key)
    headers <- if(!is.null(etag))
        list(`If-Match`=etag)
    else list()

    res <- call_table_endpoint(table$endpoint, path, body=entity, headers=headers, http_verb="PUT",
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
list_table_entities <- function(table, filter=NULL, select=NULL)
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
    val
}


#' @export
get_table_entity <- function(table, partition_key, row_key, select=NULL)
{
    path <- sprintf("%s(PartitionKey='%s',RowKey='%s')", table$name, partition_key, row_key)
    opts <- list(`$select`=paste0(select, collapse=","))
    call_table_endpoint(table$endpoint, path, options=opts)
}
