add_methods <- function()
{
    AzureRMR::az_resource_group$set("public", "create_cosmosdb_account", overwrite=TRUE,
    function(name, location=self$location,
             interface=c("sql", "mongo", "table", "cassandra", "graph"),
             serverless=FALSE, free_tier=FALSE,
             properties=list(), tags=list(), ...)
    {
        interface <- match.arg(interface)
        kind <- if(interface == "mongo") "MongoDB" else "GlobalDocumentDB"

        capabilities <- list()
        if(interface == "cassandra")
            capabilities <- c(capabilities, list(list(name="EnableCassandra")))
        else if(interface == "mongo")
            capabilities <- c(capabilities, list(
                list(name="EnableMongo"),
                list(name="DisableRateLimitingResponses")
            ))
        else if(interface == "table")
            capabilities <- c(capabilities, list(list(name="EnableTable")))
        else if(interface == "graph")
            capabilities <- c(capabilities, list(list(name="EnableGremlin")))

        if(serverless)
            capabilities <- c(capabilities, list(list(name="EnableServerless")))

        properties <- utils::modifyList(properties, list(
            databaseAccountOfferType="standard",
            enableFreeTier=free_tier,
            capabilities=capabilities,
            locations=list(
                list(
                    id=paste0(name, "-", location),
                    failoverPriority=0,
                    locationName=location
                )
            )
        ))

        default_experience <- switch(interface,
            "sql"="Core (SQL)",
            "mongo"="Azure Cosmos DB for MongoDB API",
            "table"="Azure Table",
            "cassandra"="Cassandra",
            "graph"="Gremlin (graph)"
        )

        tags <- utils::modifyList(tags, list(defaultExperience=default_experience))

        AzureCosmosR::az_cosmosdb$new(self$token, self$subscription, self$name,
            type="Microsoft.documentDB/databaseAccounts", name=name, location=location,
            kind=kind, properties=properties, tags=tags, ...)
    })

    AzureRMR::az_resource_group$set("public", "get_cosmosdb_account", overwrite=TRUE,
    function(name)
    {
        AzureCosmosR::az_cosmosdb$new(self$token, self$subscription, self$name,
            type="Microsoft.documentDB/databaseAccounts", name=name)
    })


    AzureRMR::az_resource_group$set("public", "delete_cosmosdb_account", overwrite=TRUE,
    function(name, confirm=TRUE, wait=FALSE)
    {
        self$get_cosmosdb_account(name)$delete(confirm=confirm, wait=wait)
    })
}
