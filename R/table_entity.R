#' Operations on table entities (rows)
#'
#' @param table A table object, of class `azure_table`.
#' @param entity For `insert_table_entity` and `update_table_entity`, a named list giving the properties (columns) of the entity. See 'Details' below.
#' @param data For `import_table_entities`, a data frame. See 'Details' below.
#' @param row_key,partition_key For `get_table_entity`, `update_table_entity` and `delete_table_entity`, the row and partition key values that identify the entity to get, update or delete. For `import_table_entities`, optionally the _columns_ in the imported data to treat as the row and partition keys. These will be renamed to `RowKey` and `PartitionKey` respectively.
#' @param etag For `update_table_entity` and `delete_table_entity`, an optional Etag value. If this is supplied, the update or delete operation will proceed only if the target entity's Etag matches this value. This ensures that an entity is only updated/deleted if it has not been modified since it was last retrieved.
#' @param filter,select For `list_table_entities`, optional row filter and column select expressions to subset the result with. If omitted, `list_table_entities` will return all entities in the table.
#' @param as_data_frame For `list_table_entities`, whether to return the results as a data frame, rather than a list of table rows.
#' @param batch_status_handler For `import_table_entities`, what to do if one or more of the batch operations fails. The default is to signal a warning and return a list of response objects, from which the details of the failure(s) can be determined. Set this to "pass" to ignore the failure.
#'
#' @details
#' These functions operate on rows of a table, also known as _entities_. `insert`, `get`, `update` and `delete_table_entity` operate on an individual row. `import_table_entities` bulk-inserts multiple rows of data into the table, using batch transactions. `list_table_entities` queries the table and returns multiple rows based on the `filter` and `subset` arguments.
#'
#' Table storage imposes the following requirements for properties (columns) of an entity:
#' - There must be properties named `RowKey` and `PartitionKey`, which together form the entity's unique identifier.
#' - The property `Timestamp` cannot be used (strictly speaking, it is reserved by the system).
#' - There can be at most 255 properties per entity, although different entities can have different properties.
#' - Table properties must be atomic (ie, they cannot be nested lists).
#'
#' For `insert_table_entity`, `update_table_entity` and `import_table_entities`, you can also specify JSON text representing the data to insert/update/import, instead of a list or data frame.
#' @return
#' `insert_table_entity` and `update_table_entity` return the Etag of the inserted/updated entity, invisibly.
#'
#' `get_table_entity` returns a named list of properties for the given entity.
#'
#' `list_table_entities` returns a data frame if `as_data_frame=TRUE`, and a list of entities (rows) otherwise.
#'
#' `import_table_entities` invisibly returns a named list, with one component for each value of the `PartitionKey` column. Each component contains the results of the individual operations to insert each row into the table.
#'
#' @seealso
#' [azure_table], [do_batch_transaction]
#'
#' [Understanding the table service data model](https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-the-table-service-data-model)
#' @aliases table_entity
#' @rdname table_entity
#' @export
insert_table_entity <- function(table, entity)
{
    if(is.character(entity) && jsonlite::validate(entity))
        entity <- jsonlite::fromJSON(entity, simplifyDataFrame=FALSE)
    else if(is.data.frame(entity))
    {
        if(nrow(entity) == 1) # special-case treatment for 1-row dataframes
            entity <- unclass(entity)
        else stop("Can only insert one entity at a time; use import_table_entities() to insert multiple entities",
                  call.=FALSE)
    }

    check_column_names(entity)
    headers <- list(Prefer="return-no-content")
    res <- call_table_endpoint(table$endpoint, table$name, body=entity, headers=headers, http_verb="POST",
                               http_status_handler="pass")
    httr::stop_for_status(res, storage_error_message(res))
    invisible(httr::headers(res)$ETag)
}


#' @rdname table_entity
#' @export
update_table_entity <- function(table, entity, row_key=entity$RowKey, partition_key=entity$PartitionKey, etag=NULL)
{
    if(is.character(entity) && jsonlite::validate(entity))
        entity <- jsonlite::fromJSON(entity, simplifyDataFrame=FALSE)
    else if(is.data.frame(entity))
    {
        if(nrow(entity) == 1) # special-case treatment for 1-row dataframes
            entity <- unclass(entity)
        else stop("Can only update one entity at a time", call.=FALSE)
    }

    check_column_names(entity)
    headers <- if(!is.null(etag))
        list(`If-Match`=etag)
    else list()
    path <- sprintf("%s(PartitionKey='%s',RowKey='%s')", table$name, partition_key, row_key)
    res <- call_table_endpoint(table$endpoint, table$name, body=entity, headers=headers, http_verb="POST",
                               http_status_handler="pass")
    httr::stop_for_status(res, storage_error_message(res))
    invisible(httr::headers(res)$ETag)
}


#' @rdname table_entity
#' @export
delete_table_entity <- function(table, row_key, partition_key, etag=NULL)
{
    path <- sprintf("%s(PartitionKey='%s',RowKey='%s')", table$name, partition_key, row_key)
    if(is.null(etag))
        etag <- "*"
    headers <- list(`If-Match`=etag)
    invisible(call_table_endpoint(table$endpoint, path, headers=headers, http_verb="DELETE"))
}


#' @rdname table_entity
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


#' @rdname table_entity
#' @export
get_table_entity <- function(table, row_key, partition_key, select=NULL)
{
    path <- sprintf("%s(PartitionKey='%s',RowKey='%s')", table$name, partition_key, row_key)
    opts <- if(!is.null(select))
        list(`$select`=paste0(select, collapse=","))
    else list()
    call_table_endpoint(table$endpoint, path, options=opts)
}


#' @rdname table_entity
#' @export
import_table_entities <- function(table, data, row_key=NULL, partition_key=NULL,
                                  batch_status_handler=c("warn", "stop", "message", "pass"))
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
    batch_status_handler <- match.arg(batch_status_handler)
    res <- lapply(split(data, data$PartitionKey), function(dfpart)
    {
        n <- nrow(dfpart)
        nchunks <- n %/% 100 + (n %% 100 > 0)
        reschunks <- lapply(seq_len(nchunks), function(chunk)
        {
            rows <- seq(from=(chunk-1)*100 + 1, to=min(chunk*100, n))
            dfchunk <- dfpart[rows, ]
            ops <- lapply(seq_len(nrow(dfchunk)), function(i)
                create_batch_operation(endpoint, path, body=dfchunk[i, ], headers=headers, http_verb="POST"))
            do_batch_transaction(endpoint, ops, batch_status_handler)
        })
        unlist(reschunks, recursive=FALSE)
    })
    invisible(res)
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
