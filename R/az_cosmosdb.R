#' Azure Cosmos DB account class
#'
#' Class representing an Azure Cosmos DB account. For working with the data inside the account, see [cosmos_endpoint] and [cosmos_database].
#'
#' @docType class
#' @section Methods:
#' The following methods are available, in addition to those provided by the [AzureRMR::az_resource] class:
#' - `list_keys(read_only=FALSE`): Return the access keys for this account.
#' - `regen_key(kind)`: Regenerate (change) an access key. `kind` should be one of "primary", "secondary", "primaryReadonly" or "secondaryReadonly".
#' - `get_endpoint(interface, ...)`: Return a default endpoint object for interacting with the data. See 'Endpoints' below.
#' - `get_sql_endpoint(key, key_type)`: Return an object representing the core (SQL) endpoint of the account.
#' - `get_table_endpoint(key)`: Return an object representing the table storage endpoint of the account.
#' - `get_mongo_endpoint(collection, key, mongo_options)`: Return an object representing the MongoDB enpoint of the account.
#'
#' @section Details:
#' Initializing a new object of this class can either retrieve an existing Cosmos DB resource, or create a new resource on the host. Generally, the best way to initialize an object is via the `get_cosmosdb_account` or `create_cosmosdb_account` methods of the [AzureRMR::az_resource_group] class, which handle the details automatically.
#'
#' @section Endpoints:
#' Azure Cosmos DB provides multiple APIs for accessing the data stored within the account. You choose at account creation the API that you want to use: core (SQL), table storage, Mongo DB, Apache Cassandra, or Gremlin. The following methods allow you to create an endpoint object corresponding to these APIs.
#'
#' - `get_endpoint(interface=NULL, ...)`: Return an endpoint object for interacting with the data. The default `interface=NULL` will choose the interface that you selected at account creation. Otherwise, set `interface` to one of "sql", "table", "mongo", "cassandra" or "gremlin" to create an endpoint object for that API. It's an error to select an interface that the Cosmos DB account doesn't actually provide.
#' - `get_sql_endpoint(key, key_type=c("master", "resource"))`: Return an endpoint object for the core (SQL) API, of class [cosmos_endpoint]. A master key provides full access to all the data in the account; a resource key provides access only to a chosen subset of the data.
#' - `get_table_endpoint(key)`: Return an endpoint object for the table storage API, of class [AzureTableStor::table_endpoint].
#' - `get_mongo_endpoint(key, mongo_options)`: Return an endpoint object for the MongoDB API, of class [cosmos_mongo_endpoint]. `mongo_options` should be an optional named list of parameters to set in the connection string.
#'
#' Note that AzureCosmosR provides a client framework only for the SQL API. To use the table storage API, you will also need the AzureTableStor package, and to use the MongoDB API, you will need the mongolite package. Currently, the Cassandra and Gremlin APIs are not supported.
#'
#' As an alternative to AzureCosmosR, you can also use the ODBC protocol to interface with the SQL API. By installing a suitable ODBC driver, you can then talk to Cosmos DB in a manner similar to other SQL databases. An advantage of the ODBC interface is that it fully supports cross-partition queries, unlike the REST API. A disadvantage is that it does not support nested document fields; functions like `array_contains()` cannot be used, and attempts to reference arrays and objects may return incorrect results.
#'
#' @seealso
#' [get_cosmosdb_account], [create_cosmosdb_account], [delete_cosmosdb_account]
#'
#' [cosmos_endpoint], [cosmos_database], [cosmos_container], [query_documents], [cosmos_mongo_endpoint], [AzureTableStor::table_endpoint], [mongolite::mongo]
#' @export
az_cosmosdb <- R6::R6Class("az_cosmosdb", inherit=az_resource,

public=list(

    list_keys=function(read_only=FALSE)
    {
        op <- if(read_only) "readonlykeys" else "listkeys"
        unlist(self$do_operation(op, http_verb="POST"))
    },

    regen_key=function(kind=c("primary", "secondary", "primaryReadonly", "secondaryReadonly"))
    {
        kind <- match.arg(kind)
        self$do_operation("regenerateKey", body=list(keyKind=kind), http_verb="POST")
        invisible(self)
    },

    get_endpoint=function(interface=NULL, ...)
    {
        if(is.null(interface))
            interface <- tolower(self$properties$EnabledApiTypes)

        if(grepl("table", interface))
            self$get_table_endpoint(...)
        else if(grepl("mongo", interface))
            self$get_mongo_endpoint(...)
        else if(grepl("cassandra", interface))
            self$get_cassandra_endpoint(...)
        else if(grepl("gremlin", interface))
            self$get_graph_endpoint(...)
        else self$get_sql_endpoint(...)
    },

    get_sql_endpoint=function(key=self$list_keys()[[1]], key_type=c("master", "resource"))
    {
        private$assert_has_sql()
        key_type <- match.arg(key_type)
        cosmos_endpoint(self$properties$documentEndpoint, key=key, key_type=key_type)
    },

    get_table_endpoint=function(key=self$list_keys()[[1]])
    {
        private$assert_has_table()
        if(!requireNamespace("AzureTableStor"))
            stop("AzureTableStor package must be installed to use the table endpoint", call.=FALSE)
        AzureTableStor::table_endpoint(self$properties$tableEndpoint, key=key)
    },

    get_mongo_endpoint=function(key=self$list_keys()[[1]], mongo_options=list())
    {
        private$assert_has_mongo()
        assert_mongolite_installed()
        cosmos_mongo_endpoint(self$properties$mongoEndpoint, key, mongo_options)
    },

    get_cassandra_endpoint=function(...)
    {
        stop("Cassandra endpoint is not yet implemented", call.=FALSE)
    },

    get_graph_endpoint=function(...)
    {
        stop("Graph (Gremlin) endpoint is not yet implemented", call.=FALSE)
    }
),

private=list(

    assert_has_sql=function()
    {
        if(is.null(self$properties$documentEndpoint))
            stop("Not a SQL-enabled Cosmos DB account", call.=FALSE)
    },

    assert_has_cassandra=function()
    {
        if(is.null(self$properties$cassandraEndpoint))
            stop("Not a Cassandra-enabled Cosmos DB account", call.=FALSE)
    },

    assert_has_mongo=function()
    {
        if(is.null(self$properties$mongoEndpoint))
            stop("Not a MongoDB-enabled Cosmos DB account", call.=FALSE)
    },

    assert_has_table=function()
    {
        if(is.null(self$properties$tableEndpoint))
            stop("Not a table storage-enabled Cosmos DB account", call.=FALSE)
    },

    assert_has_graph=function()
    {
        if(is.null(self$properties$gremlinEndpoint))
            stop("Not a Gremlin-enabled Cosmos DB account", call.=FALSE)
    }
))
