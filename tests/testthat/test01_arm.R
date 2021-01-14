tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Authentication tests skipped: ARM credentials not set")


sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)

rgname <- make_name()
rg <- sub$create_resource_group(rgname, location="australiaeast")

test_that("ARM interface works",
{
    acctname <- make_name()
    expect_is(rg$create_cosmosdb_account(acctname, wait=TRUE), "az_cosmosdb")
    acct <- rg$get_cosmosdb_account(acctname)
    expect_is(acct, "az_cosmosdb")
    expect_is(acct$list_keys(), "character")
    expect_silent(acct$regen_key())
    expect_is(acct$get_endpoint(), "cosmos_endpoint")

    Sys.sleep(20)
    rg$delete_cosmosdb_account(acctname, confirm=FALSE)

    # serverless
    acctname <- make_name()
    expect_is(rg$create_cosmosdb_account(acctname, serverless=TRUE, wait=TRUE), "az_cosmosdb")

    # mongo
    acctname <- make_name()
    macct <- rg$create_cosmosdb_account(acctname, serverless=TRUE, interface="mongo", wait=TRUE)
    expect_is(macct, "az_cosmosdb")
    expect_is(macct$get_endpoint(), "cosmos_mongo_endpoint")
    expect_is(macct$get_sql_endpoint(), "cosmos_endpoint")

    # table
    acctname <- make_name()
    tacct <- rg$create_cosmosdb_account(acctname, serverless=TRUE, interface="table", wait=TRUE)
    expect_is(tacct, "az_cosmosdb")
    expect_is(tacct$get_endpoint(), "table_endpoint")
    expect_is(tacct$get_sql_endpoint(), "cosmos_endpoint")

    # cassandra
    acctname <- make_name()
    cacct <- rg$create_cosmosdb_account(acctname, serverless=TRUE, interface="cassandra", wait=TRUE)
    expect_is(cacct, "az_cosmosdb")
    expect_error(cacct$get_endpoint())

    # graph
    acctname <- make_name()
    gacct <- rg$create_cosmosdb_account(acctname, serverless=TRUE, interface="graph", wait=TRUE)
    expect_is(gacct, "az_cosmosdb")
    expect_error(gacct$get_endpoint())

    lst <- rg$list_cosmosdb_accounts()
    expect_is(lst, "list")
    expect_true(all(sapply(lst, inherits, "az_cosmosdb")))
})

teardown({
    suppressMessages(rg$delete(confirm=FALSE))
})
