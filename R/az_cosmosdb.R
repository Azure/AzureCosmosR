#' @export
az_cosmosdb <- R6::R6Class("az_cosmosdb", inherit=AzureRMR::az_resource,

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

    get_endpoint=function(key=self$list_keys()[[1]], key_type=c("master", "resource"), interface=NULL)
    {
        key_type <- match.arg(key_type)
        if(is.null(interface))
            interface <- tolower(self$properties$EnabledApiTypes)

        if(grepl("table", interface))
            self$get_table_endpoint(key)
        else if(interface == "sql")
            self$get_sql_endpoint(key, key_type)
        else stop("Other API endpoints not yet implemented", call.=FALSE)
    },

    get_sql_endpoint=function(key=self$list_keys()[[1]], key_type=c("master", "resource"))
    {
        private$assert_has_sql()
        key_type <- match.arg(key_type)
        cosmos_endpoint(self$properties$documentEndpoint, key=key, key_type=key_type)
    },

    get_table_endpoint=function(key=self$list_keys()[[1]], key_type)
    {
        private$assert_has_table()
        AzureTableStor::table_endpoint(self$properties$tableEndpoint, key=key)
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
