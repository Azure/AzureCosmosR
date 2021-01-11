tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Database tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_COSMOSDB_RG")
acctname <- Sys.getenv("AZ_TEST_COSMOSDB_ACCT")

if(rgname == "" || acctname == "")
    skip("Database tests skipped: resource details not set")

cosmos <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$
    get_subscription(subscription)$
    get_resource_group(rgname)$
    get_cosmosdb_account(acctname)


test_that("Database and container methods work",
{
    endp <- cosmos$get_endpoint()
    expect_is(endp, "cosmos_endpoint")

    dbname <- paste(make_name(5), make_name(5))
    db <- create_cosmos_database(endp, dbname)
    expect_is(db, "cosmos_database")

    db2 <- get_cosmos_database(endp, dbname)
    expect_is(db2, "cosmos_database")
    expect_identical(db, db2)

    lst <- list_cosmos_databases(endp)
    expect_is(lst, "list")
    expect_true(all(sapply(lst, inherits, "cosmos_database")))

    contname <- paste(make_name(5), make_name(5))
    cont <- create_cosmos_container(db, contname, partition_key="Species")
    expect_is(cont, "cosmos_container")

    cont2 <- get_cosmos_container(db, contname)
    expect_identical(cont, cont2)

    lst2 <- list_cosmos_containers(db)
    expect_is(lst2, "list")
    expect_true(all(sapply(lst2, inherits, "cosmos_container")))

    key <- get_partition_key(cont)
    expect_is(key, "character")
})


teardown({
    endp <- cosmos$get_endpoint()
    lst <- list_cosmos_databases(endp)
    lapply(lst, delete_cosmos_database, confirm=FALSE)
})

