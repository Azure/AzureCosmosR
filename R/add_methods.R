add_methods <- function()
{
    AzureRMR::az_resource_group$set("public", "create_cosmosdb_account", overwrite=TRUE,
    function(name, location=self$location,
             interface=c("sql", "cassandra", "mongo", "table", "graph"),
             serverless=FALSE, free_tier=FALSE,
             properties=list(), ...)
    {
        interface <- match.arg(interface)
        kind <- if(interface == "mongo") "MongoDB" else "GlobalDocumentDB"

        capabilities <- if(interface == "cassandra")
            list(list(name="EnableCassandra"))
        else if(interface == "mongo")
            list(
                list(name="EnableMongo"),
                list(name="DisableRateLimitingResponses")
            )
        else if(interface == "table")
            list(list(name="EnableTable"))
        else if(interface == "graph")
            list(list(name="EnableGremlin"))
        else list()

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

        AzureCosmosR::az_cosmosdb$new(self$token, self$subscription, self$name,
            type="Microsoft.documentDB/databaseAccounts", name=name, location=location,
            kind=kind, properties=properties, ...)
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
