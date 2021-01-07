tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Stored procedure tests skipped: ARM credentials not set")

rgname <- Sys.getenv("AZ_TEST_COSMOSDB_RG")
acctname <- Sys.getenv("AZ_TEST_COSMOSDB_ACCT")

if(rgname == "" || acctname == "")
    skip("Stored procedure tests skipped: resource details not set")

cosmos <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$
    get_subscription(subscription)$
    get_resource_group(rgname)$
    get_cosmosdb_account(acctname)
endp <- cosmos$get_endpoint()


test_that("Stored procedure methods work",
{
    db <- create_cosmos_database(endp, make_name())
    cont <- create_cosmos_container(db, make_name(), partition_key="Species")

    sproc <- create_stored_procedure(cont, "helloworld", "../resources/helloworld.js")
    expect_is(sproc, "cosmos_stored_procedure")

    sproc2 <- get_stored_procedure(cont, "helloworld")
    expect_is(sproc2, "cosmos_stored_procedure")
    expect_identical(sproc, sproc2)

    expect_identical(exec_stored_procedure(cont, "helloworld"), "Hello, World")
    expect_identical(exec_stored_procedure(sproc), "Hello, World")

    src <- readLines("../resources/helloworld.js")
    sproc3 <- create_stored_procedure(cont, "helloworld_copy", src)
    expect_identical(exec_stored_procedure(sproc3), "Hello, World")

    lst <- list_stored_procedures(cont)
    expect_true(is.list(lst) && length(lst) == 2)
    expect_true(all(sapply(lst, inherits, "cosmos_stored_procedure")))

    expect_silent(delete_stored_procedure(sproc3, confirm=FALSE))
    expect_silent(delete_stored_procedure(cont, "helloworld", confirm=FALSE))
    expect_error(delete_stored_procedure(sproc, confirm=FALSE))

    sproc4 <- create_stored_procedure(cont, "helloworld2", "../resources/helloworld2.js")
    res <- exec_stored_procedure(sproc4, list("me"))
    expect_identical(res, "Hello, me")
})


teardown({
    lst <- list_cosmos_databases(endp)
    lapply(lst, delete_cosmos_database, confirm=FALSE)
})
