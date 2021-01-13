tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("UDF tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_COSMOSDB_RG")
acctname <- Sys.getenv("AZ_TEST_COSMOSDB_ACCT")

if(rgname == "" || acctname == "")
    skip("UDF tests skipped: resource details not set")

cosmos <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$
    get_subscription(subscription)$
    get_resource_group(rgname)$
    get_cosmosdb_account(acctname)
endp <- cosmos$get_endpoint()


test_that("UDF methods work",
{
    db <- create_cosmos_database(endp, make_name())
    cont <- create_cosmos_container(db, make_name(), partition_key="Species")

    udf <- create_udf(cont, "times2", "function(x) { return 2*x; }")
    expect_is(udf, "cosmos_udf")

    udf2 <- get_udf(cont, "times2")
    expect_is(udf2, "cosmos_udf")
    expect_identical(udf, udf2)

    expect_silent(bulk_import(cont, iris, verbose=FALSE))

    query <- 'select udf.times2(c["Sepal.Length"]) from cont c where c["Sepal.Width"] > 3'
    df <- query_documents(cont, query)

    expect_is(df, "data.frame")
    expect_identical(nrow(df), nrow(subset(iris, Sepal.Width > 3)))
    expect_identical(df[[1]], subset(iris, Sepal.Width > 3)$Sepal.Length * 2)

    lst <- list_udfs(cont)
    expect_true(is.list(lst) && length(lst) == 1)
    expect_true(all(sapply(lst, inherits, "cosmos_udf")))

    expect_silent(delete_udf(udf, confirm=FALSE))
})


teardown({
    lst <- list_cosmos_databases(endp)
    lapply(lst, delete_cosmos_database, confirm=FALSE)
})
