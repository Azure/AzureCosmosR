#' @import AzureRMR
NULL

utils::globalVariables(c("self", "private"))


.onLoad <- function(libname, pkgname)
{
    options(azure_cosmosdb_api_version="2018-12-31")
    add_methods()
}


delete_confirmed <- function(confirm, name, type)
{
    if(!interactive() || !confirm)
        return(TRUE)
    msg <- sprintf("Are you sure you really want to delete the %s '%s'?", type, name)
    ok <- if(getRversion() < numeric_version("3.5.0"))
    {
        msg <- paste(msg, "(yes/No/cancel) ")
        yn <- readline(msg)
        if (nchar(yn) == 0)
            FALSE
        else tolower(substr(yn, 1, 1)) == "y"
    }
    else utils::askYesNo(msg, FALSE)
    isTRUE(ok)
}

