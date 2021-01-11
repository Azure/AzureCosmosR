tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Container querying tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_COSMOSDB_RG")
acctname <- Sys.getenv("AZ_TEST_COSMOSDB_ACCT")

if(rgname == "" || acctname == "")
    skip("Container querying tests skipped: resource details not set")

cosmos <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$
    get_subscription(subscription)$
    get_resource_group(rgname)$
    get_cosmosdb_account(acctname)
endp <- cosmos$get_endpoint()


test_that("Container querying methods work",
{
    db <- create_cosmos_database(endp, make_name())
    expect_is(db, "cosmos_database")

    cont <- create_cosmos_container(db, make_name(), partition_key="Species")
    expect_is(cont, "cosmos_container")

    bulk_import(cont, iris)
    lst <- list_documents(cont)
    expect_is(lst, "list")
    expect_true(length(lst) == nrow(iris) && all(sapply(lst, inherits, "cosmos_document")))

    id1 <- lst[[1]]$data$id
    key1 <- lst[[1]]$data$Species
    doc1 <- get_document(cont, id1, partition_key=key1)
    expect_identical(doc1, lst[[1]])

    doc2 <- get_document(cont, id1, partition_key=key1, metadata=FALSE)
    expect_identical(doc1$data[names(iris)], doc2$data)

    expect_error(get_document(cont, id1, partition_key="bad"))

    query <- "select * from cont"
    df <- query_documents(cont, query, metadata=FALSE)
    expect_is(df, "data.frame")
    expect_identical(dim(df), dim(iris))

    df_part <- query_documents(cont, query, partition_key="setosa", metadata=FALSE)
    expect_is(df_part, "data.frame")
    expect_identical(dim(df_part), dim(subset(iris, Species == "setosa")))

    query2 <- 'select c["Sepal.Length"] from cont c where c["Sepal.Width"] > 3'
    df2 <- query_documents(cont, query2, metadata=FALSE)
    expect_identical(dim(df2), dim(subset(iris, Sepal.Width > 3, Sepal.Length)))

    keys <- list_partition_key_values(cont)
    expect_length(keys, length(unique(iris$Species)))

    pkranges <- list_partition_key_ranges(cont)
    expect_is(pkranges, "character")
    expect_true(length(pkranges) >= 1)

    expect_silent(delete_document(doc1, confirm=FALSE))
    expect_length(list_documents(cont), nrow(iris) - 1)
})


teardown({
    lst <- list_cosmos_databases(endp)
    lapply(lst, delete_cosmos_database, confirm=FALSE)
})
