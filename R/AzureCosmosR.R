#' @import AzureRMR
#' @import AzureStor
NULL

# assorted imports of friend functions
sign_sha256 <- get("sign_sha256", getNamespace("AzureStor"))

is_endpoint_url <- get("is_endpoint_url", getNamespace("AzureStor"))

delete_confirmed <- get("delete_confirmed", getNamespace("AzureStor"))

storage_error_message <- get("storage_error_message", getNamespace("AzureStor"))
