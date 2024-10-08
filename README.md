# AzureCosmosR <img src="man/figures/logo.png" align="right" width=150 />

[![CRAN](https://www.r-pkg.org/badges/version/AzureCosmosR)](https://cran.r-project.org/package=AzureCosmosR)
![Downloads](https://cranlogs.r-pkg.org/badges/AzureCosmosR)
![R-CMD-check](https://github.com/Azure/AzureCosmosR/workflows/R-CMD-check/badge.svg)

An interface to [Azure Cosmos DB](https://azure.microsoft.com/en-us/services/cosmos-db/), a NoSQL database service from Microsoft.

> Azure Cosmos DB is a fully managed NoSQL database for modern app development. Single-digit millisecond response times, and automatic and instant scalability, guarantee speed at any scale. Business continuity is assured with SLA-backed availability and enterprise-grade security. App development is faster and more productive thanks to turnkey multi region data distribution anywhere in the world, open source APIs and SDKs for popular languages. As a fully managed service, Azure Cosmos DB takes database administration off your hands with automatic management, updates and patching. It also handles capacity management with cost-effective serverless and automatic scaling options that respond to application needs to match capacity with demand.

On the Resource Manager side, AzureCosmosR extends the [AzureRMR](https://cran.r-project.org/package=AzureRMR) class framework to allow creating and managing Cosmos DB accounts. On the client side, it provides a comprehensive interface to the Cosmos DB SQL/core API as well as bridges to the MongoDB and table storage APIs.

The primary repo for this package is at https://github.com/Azure/AzureCosmosR; please submit issues and PRs there. It is also mirrored at the Cloudyr org at https://github.com/cloudyr/AzureCosmosR. You can install the development version of the package with `devtools::install_github("Azure/AzureCosmosR")`.

## SQL interface

### AAD authentication

According to the latest security guides for Azure Cosmos DB, role-based access control is suggested in favor over key-based and resource owner password credentials-based authentication: https://learn.microsoft.com/en-us/azure/cosmos-db/table/security/.

Below, we provide an example of how AAD token-based authentication can be done.
```r
library(AzureAuth)
library(AzureCosmosR)

token <- AzureAuth::get_managed_token("https://cosmos.azure.com")
endpoint <- AzureCosmosR::cosmos_endpoint("https://myaccount.documents.azure.com:443/", key = token, key_type = "aad")

list_cosmos_databases(endpoint)

db <- get_cosmos_database(endpoint, "mydatabase")

#... other operations
```

### Key based authentication

AzureCosmosR provides a suite of methods to work with databases, containers (tables) and documents (rows) using the SQL API.

```r
library(dplyr)
library(AzureCosmosR)

endp <- cosmos_endpoint("https://myaccount.documents.azure.com:443/", key="mykey")

list_cosmos_databases(endp)

db <- get_cosmos_database(endp, "mydatabase")

# create a new container and upload the Star Wars dataset from dplyr
cont <- create_cosmos_container(db, "mycontainer", partition_key="sex")
bulk_import(cont, starwars)

query_documents(cont, "select * from mycontainer")

# an array select: all characters who appear in ANH
query_documents(cont,
    "select c.name
        from mycontainer c
        where array_contains(c.films, 'A New Hope')")
```

You can easily create and execute stored procedures and user-defined functions:

```r
proc <- create_stored_procedure(
    cont,
    "helloworld",
    'function () {
        var context = getContext();
        var response = context.getResponse();
        response.setBody("Hello, World");
    }'
)

exec_stored_procedure(proc)

create_udf(cont, "times2", "function(x) { return 2*x; }")

query_documents(cont, "select udf.times2(c.height) from cont c")
```

Aggregates take some extra work, as the Cosmos DB REST API only has limited support for cross-partition queries. Set `by_pkrange=TRUE` in the `query_documents` call, which will run the query on each partition key range (pkrange) and return a list of data frames. You can then process the list to obtain an overall result.

```r
# average height by sex, by pkrange
df_lst <- query_documents(
    cont,
    "select c.gender, count(1) n, avg(c.height) height
        from mycontainer c
        group by c.gender",
    by_pkrange=TRUE
)

# combine pkrange results
df_lst %>%
    bind_rows(.id="pkrange") %>%
    group_by(gender) %>%
    summarise(height=weighted.mean(height, n))
```

## Other client interfaces

### MongoDB

You can query data in a MongoDB-enabled Cosmos DB instance using the mongolite package. AzureCosmosR provides a simple bridge to facilitate this.

```r
endp <- cosmos_mongo_endpoint("https://myaccount.mongo.cosmos.azure.com:443/", key="mykey")

# a mongolite::mongo object
conn <- cosmos_mongo_connection(endp, "mycollection", "mydatabase")
conn$find("{}")
```

For more information on working with MongoDB, see the [mongolite](https://jeroen.github.io/mongolite/) documentation.

### Table storage

You can work with data in a table storage-enabled Cosmos DB instance using the AzureTableStor package.

```r
endp <- AzureTableStor::table_endpoint("https://myaccount.table.cosmos.azure.com:443/", key="mykey")

tab <- AzureTableStor::storage_table(endp, "mytable")
AzureTableStor::list_table_entities(tab, filter="firstname eq 'Satya'")
```

## Azure Resource Manager interface

On the ARM side, AzureCosmosR extends the AzureRMR class framework with a new `az_cosmosdb` class representing a Cosmos DB account resource, and methods for the `az_resource_group` resource group class.

```r
rg <- AzureRMR::get_azure_login()$
    get_subscription("sub_id")$
    get_resource_group("rgname")

rg$create_cosmosdb_account("mycosmosdb", interface="sql", free_tier=TRUE)
rg$list_cosmosdb_accounts()
cosmos <- rg$get_cosmosdb_account("mycosmosdb")

# access keys (passwords) for this account
cosmos$list_keys()

# get an endpoint object -- detects which API this account uses
endp <- cosmos$get_endpoint()

# API-specific endpoints
cosmos$get_sql_endpoint()
cosmos$get_mongo_endpoint()
cosmos$get_table_endpoint()
```

----
<p align="center"><a href="https://github.com/Azure/AzureR"><img src="https://github.com/Azure/AzureR/raw/master/images/logo2.png" width=800 /></a></p>

