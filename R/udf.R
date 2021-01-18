#' Methods for working with Azure Cosmos DB user-defined functions
#'
#' @param object A Cosmos DB container object, as obtained by `get_cosmos_container` or `create_cosmos_container`, or for `delete_udf.cosmos_udf`, the function object.
#' @param funcname The name of the user-defined function.
#' @param body For `create_udf` and `replace_udf`, the body of the function. This can be either a character string containing the source code, or the name of a source file.
#' @param confirm For `delete_udf`, whether to ask for confirmation before deleting.
#' @param ... Optional arguments passed to lower-level functions.
#' @details
#' These are methods for working with user-defined functions (UDFs) in Azure Cosmos DB using the core (SQL) API. In the Cosmos DB model, UDFs are written in JavaScript and associated with a container.
#'
#' @return
#' For `get_udf` and `create_udf`, an object of class `cosmos_udf`. For `list_udfs`, a list of such objects.
#' @seealso
#' [cosmos_container], [get_stored_procedure]
#' @examples
#' \dontrun{
#'
#' endp <- cosmos_endpoint("https://myaccount.documents.azure.com:443/", key="mykey")
#' db <- get_cosmos_database(endp, "mydatabase")
#'
#' # importing the Star Wars data from dplyr
#' cont <- endp %>%
#'     get_cosmos_database(endp, "mydatabase") %>%
#'     create_cosmos_container(db, "mycontainer", partition_key="sex")
#'
#' create_udf(cont, "times2", "function(x) { return 2*x; }")
#'
#' list_udfs(cont)
#'
#' # UDFs in queries are prefixed with the 'udf.' identifier
#' query_documents(cont, "select udf.times2(c.height) t2 from cont c")
#'
#' delete_udf(cont, "times2")
#'
#' }
#' @rdname cosmos_udf
#' @export
get_udf <- function(object, ...)
{
    UseMethod("get_udf")
}

#' @rdname cosmos_udf
#' @export
get_udf.cosmos_container <- function(object, funcname, ...)
{
    path <- file.path("udfs", funcname)
    res <- do_cosmos_op(object, path, "udfs", path, ...)
    udf <- process_cosmos_response(res)
    as_udf(udf, object)
}

#' @rdname cosmos_udf
#' @export
list_udfs <- function(object, ...)
{
    UseMethod("list_udfs")
}

#' @export
list_udfs.cosmos_container <- function(object, ...)
{
    res <- do_cosmos_op(object, "udfs", "udfs", "", ...)
    atts <- if(inherits(res, "response"))
        process_cosmos_response(res)$UserDefinedFunctions
    else unlist(lapply(process_cosmos_response(res), `[[`, "UserDefinedFunctions"), recursive=FALSE)
    lapply(atts, as_udf, container=object)
}


#' @rdname cosmos_udf
#' @export
create_udf <- function(object, ...)
{
    UseMethod("create_udf")
}

#' @rdname cosmos_udf
#' @export
create_udf.cosmos_container <- function(object, funcname, body, ...)
{
    if(is.character(body) && length(body) == 1 && file.exists(body))
        body <- readLines(body)
    body <- list(id=funcname, body=paste0(body, collapse="\n"))
    res <- do_cosmos_op(object, "udfs", "udfs", "", body=body, encode="json", http_verb="POST", ...)
    udf <- process_cosmos_response(res)
    invisible(as_udf(udf, object))
}


#' @rdname cosmos_udf
#' @export
replace_udf <- function(object, ...)
{
    UseMethod("replace_udf")
}

#' @rdname cosmos_udf
#' @export
replace_udf.cosmos_container <- function(object, funcname, body, ...)
{
    body <- list(id=funcname, body=body)
    path <- file.path("udfs", funcname)
    res <- do_cosmos_op(object, path, "udfs", path, body=body, encode="json", http_verb="PUT", ...)
    udf <- process_cosmos_response(res)
    invisible(as_udf(udf, object))
}

#' @rdname cosmos_udf
#' @export
replace_udf.cosmos_udf <- function(object, body, ...)
{
    replace_udf.cosmos_container(object$container, object$id, body, ...)
}


#' @rdname cosmos_udf
#' @export
delete_udf <- function(object, ...)
{
    UseMethod("delete_udf")
}

#' @rdname cosmos_udf
#' @export
delete_udf.cosmos_container <- function(object, funcname, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, funcname, "stored procedure"))
        return(invisible(NULL))

    path <- file.path("udfs", funcname)
    res <- do_cosmos_op(object, path, "udfs", path, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res))
}

#' @rdname cosmos_udf
#' @export
delete_udf.cosmos_udf <- function(object, ...)
{
    delete_udf(object$container, object$id, ...)
}


#' @export
print.cosmos_udf <- function(x, ...)
{
    cat("Cosmos DB SQL user defined function '", x$id, "'\n", sep="")
    invisible(x)
}


as_udf <- function(udf, container)
{
    udf$container <- container
    structure(udf, class="cosmos_udf")
}
