#' @import AzureRMR
NULL

.onLoad <- function(libname, pkgname)
{
    options(azure_cosmosdb_api_version="2018-12-31")
}
