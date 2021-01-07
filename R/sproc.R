#' Methods for working with Azure Cosmos DB stored procedures
#'
#' @param object A Cosmos DB container object, as obtained by `get_cosmos_container` or `create_cosmos_container`, or for `delete_stored_procedure.cosmos_stored_procedure`, the stored procedure object.
#' @param procname The name of the stored procedure.
#' @param body For `create_stored_procedure` and `replace_stored_procedure`, the body of the stored procedure. This can be either a character string containing the source code, or the name of a source file.
#' @param parameters For `exec_stored_procedure`, a list of parameters to pass to the procedure.
#' @param confirm For `delete_cosmos_container`, whether to ask for confirmation before deleting.
#' @param ... Optional arguments passed to lower-level functions.
#' @details
#' These are methods for working with stored procedures in Azure Cosmos DB using the core (SQL) API. In the Cosmos DB model, stored procedures are written in JavaScript and associated with a container.
#' @examples
#' \dontrun{
#'
#' endp <- cosmos_endpoint("https://myaccount.documents.azure.com:443/", key="mykey")
#' db <- get_cosmos_database(endp, "mydatabase")
#' cont <- create_cosmos_container(db, "mycontainer", partition_key="sex")
#'
#' # a simple stored procedure
#' src <- 'function helloworld() {
#'    var context = getContext();
#'     var response = context.getResponse();
#'     response.setBody("Hello, World");
#' }'
#' create_stored_procedure(cont, "helloworld", src)
#' sproc <- get_stored_procedure(cont, "helloworld")
#' exec_stored_procedure(sproc)
#'
#' # more complex example: uploading data
#' sproc2 <- create_stored_procedure(cont, "myBulkUpload",
#'     body=system.file("srcjs/bulkUpload.js", package="AzureCosmosR"))
#'
#' list_stored_procedures(cont)
#'
#' sw_male <- dplyr::filter(dplyr::starwars, sex == "male")
#' exec_stored_procedure(sproc2, parameters=list(sw_male))
#'
#' delete_stored_procedure(sproc)
#' delete_stored_procedure(sproc2)
#'
#' }
#' @aliases cosmos_stored_procedure
#' @rdname cosmos_stored_procedure
#' @export
get_stored_procedure <- function(object, ...)
{
    UseMethod("get_stored_procedure")
}

#' @rdname cosmos_stored_procedure
#' @export
get_stored_procedure.cosmos_container <- function(object, procname, ...)
{
    path <- file.path("sprocs", procname)
    res <- do_cosmos_op(object, path, "sprocs", path, ...)
    sproc <- process_cosmos_response(res)
    as_stored_procedure(sproc, object)
}

#' @rdname cosmos_stored_procedure
#' @export
list_stored_procedures <- function(object, ...)
{
    UseMethod("list_stored_procedures")
}

#' @export
list_stored_procedures.cosmos_container <- function(object, ...)
{
    res <- do_cosmos_op(object, "sprocs", "sprocs", "", ...)
    atts <- if(inherits(res, "response"))
        process_cosmos_response(res)$StoredProcedures
    else unlist(lapply(process_cosmos_response(res), `[[`, "StoredProcedures"), recursive=FALSE)
    lapply(atts, as_stored_procedure, container=object)
}


#' @rdname cosmos_stored_procedure
#' @export
create_stored_procedure <- function(object, ...)
{
    UseMethod("create_stored_procedure")
}

#' @rdname cosmos_stored_procedure
#' @export
create_stored_procedure.cosmos_container <- function(object, procname, body, ...)
{
    if(is.character(body) && length(body) == 1 && file.exists(body))
        body <- readLines(body)
    body <- list(id=procname, body=paste0(body, collapse="\n"))
    res <- do_cosmos_op(object, "sprocs", "sprocs", "", body=body, encode="json", http_verb="POST", ...)
    sproc <- process_cosmos_response(res)
    invisible(as_stored_procedure(sproc, object))
}


#' @rdname cosmos_stored_procedure
#' @export
exec_stored_procedure <- function(object, ...)
{
    UseMethod("exec_stored_procedure")
}

#' @rdname cosmos_stored_procedure
#' @export
exec_stored_procedure.cosmos_container <- function(object, procname, parameters=list(), ...)
{
    path <- file.path("sprocs", procname)
    res <- do_cosmos_op(object, path, "sprocs", path, body=parameters, encode="json", http_verb="POST", ...)
    process_cosmos_response(res)
}

#' @rdname cosmos_stored_procedure
#' @export
exec_stored_procedure.cosmos_stored_procedure <- function(object, ...)
{
    exec_stored_procedure(object$container, object$id, ...)
}


#' @rdname cosmos_stored_procedure
#' @export
replace_stored_procedure <- function(object, ...)
{
    UseMethod("replace_stored_procedure")
}

#' @rdname cosmos_stored_procedure
#' @export
replace_stored_procedure.cosmos_container <- function(object, procname, body, ...)
{
    body <- list(id=procname, body=body)
    path <- file.path("sprocs", procname)
    res <- do_cosmos_op(object, path, "sprocs", path, body=body, encode="json", http_verb="PUT", ...)
    sproc <- process_cosmos_response(res)
    invisible(as_stored_procedure(sproc, object))
}

#' @rdname cosmos_stored_procedure
#' @export
replace_stored_procedure.cosmos_stored_procedure <- function(object, body, ...)
{
    replace_stored_procedure.cosmos_container(object$container, object$id, body, ...)
}


#' @rdname cosmos_stored_procedure
#' @export
delete_stored_procedure <- function(object, ...)
{
    UseMethod("delete_stored_procedure")
}

#' @rdname cosmos_stored_procedure
#' @export
delete_stored_procedure.cosmos_container <- function(object, procname, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, procname, "stored procedure"))
        return(invisible(NULL))

    path <- file.path("sprocs", procname)
    res <- do_cosmos_op(object, path, "sprocs", path, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res))
}

#' @rdname cosmos_stored_procedure
#' @export
delete_stored_procedure.cosmos_stored_procedure <- function(object, ...)
{
    delete_stored_procedure(object$container, object$id, ...)
}


#' @export
print.cosmos_stored_procedure <- function(x, ...)
{
    cat("Cosmos DB SQL stored procedure '", x$id, "'\n", sep="")
    invisible(x)
}


as_stored_procedure <- function(sproc, container)
{
    sproc$container <- container
    structure(sproc, class="cosmos_stored_procedure")
}
