#' MongoDB endpoint for Azure Cosmos DB
#'
#' @param host for `cosmos_mongo_endpoint`, the URL of the Cosmos DB MongoDB endpoint. Usually of the form "https://{account-name}.mongo.cosmos.azure.com:443/".
#' @param key A string containing the access key (password) for the endpoint. Can be either a read-write or read-only key.
#' @param mongo_options A named list containing any additional parameters for the Mongo DB connection string.
#' @param connection_string Optionally, the full connection string for the MongoDB endpoint. If this is supplied, all other arguments are ignored.
#' @param endpoint For `cosmos_mongo_connection`, a MongoDB endpoint object as obtained from `cosmos_mongo_endpoint`.
#' @param collection,database For `cosmos_mongo_connection`, the collection and database to connect to.
#' @param ... Optional arguments passed to lower-level functions.
#' @details
#' These functions are the basis of the AzureCosmosR MongoDB API client. They serve as a bridge between the Azure endpoint and the MongoDB connection functionality provided by the mongolite package.
#' @return
#' For `cosmos_mongo_endpoint`, an object of S3 class `cosmos_mongo_endpoint`.
#'
#' For `cosmos_mongo_connection`, an object of class `mongolite::mongo` which can then be used to interact with the given collection.
#' @seealso
#' [az_cosmosdb], [mongolite::mongo]
#'
#' For the SQL API client framework: [cosmos_endpoint], [cosmos_database], [cosmos_container], [query_documents]
#' @rdname cosmos_mongo
#' @export
cosmos_mongo_endpoint <- function(host, key, mongo_options=list(), connection_string=NULL)
{
    assert_mongolite_installed()
    if(is.null(connection_string))
    {
        url <- httr::parse_url(host)
        url$port <- 10255
        url$scheme <- "mongodb"
        url$username <- regmatches(url$hostname, regexpr("^[^.]+", url$hostname))
        url$password <- key
        url$query <- utils::modifyList(
            list(
                ssl=TRUE,
                replicaSet="globaldb",
                retrywrites=FALSE,
                maxIdleTimeMS=120000
            ),
            mongo_options
        )
    }
    else url <- httr::parse_url(connection_string)
    structure(list(host=url, key=key), class="cosmos_mongo_endpoint")
}

#' @export
print.cosmos_mongo_endpoint <- function(x, ...)
{
    cat("Cosmos DB MongoDB endpoint\n")
    orig_host <- x$host
    orig_host$username <- orig_host$password <- NULL
    orig_host$query <- list()
    cat("Host:", httr::build_url(orig_host), "\n")
    invisible(x)
}

#' @rdname cosmos_mongo
#' @export
cosmos_mongo_connection <- function(endpoint, ...)
{
    assert_mongolite_installed()
    UseMethod("cosmos_mongo_connection")
}

#' @rdname cosmos_mongo
#' @export
cosmos_mongo_connection.cosmos_mongo_endpoint <- function(endpoint, collection, database, ...)
{
    mongolite::mongo(collection=collection, db=database, url=httr::build_url(endpoint$host), ...)
}


assert_mongolite_installed <- function()
{
    if(!requireNamespace("mongolite"))
        stop("mongolite package must be installed to use the MongoDB endpoint", call.=FALSE)
}
