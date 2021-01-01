#' Methods for working with Azure Cosmos DB stored procedures
#'
#' @param container A Cosmos DB container object, as obtained by `get_cosmos_container` or `create_cosmos_container`, or for `delete_stored_procedure.cosmos_stored_procedure, the stored procedure object.
#' @param name The name of the stored procedure.
#' @param body For `create_stored_procedure` and `replace_stored_procedure`, the body of the stored procedure as text.
#' @param parameters For `call_stored_procedure`, a list of parameters to pass to the procedure.
#' @param confirm For `delete_cosmos_container`, whether to ask for confirmation before deleting.
#' @param ... Optional arguments passed to lower-level functions.
#' @details
#' These are methods for working with stored procedures in Azure Cosmos DB using the core (SQL) API. In the Cosmos DB model, stored procedures are written in JavaScript and associated with a container.
#' @examples
#' \dontrun{
#'
#' # example text of a stored procedure: uploading multiple rows of data
#' readLines(system.file("srcjs/bulkUpload.js", package="AzureCosmosR"))
#'
#' endp <- cosmos_endpoint("https://myaccount.documents.azure.com:443/", key="mykey")
#' db <- get_cosmos_database(endp, "mydatabase")
#' cont <- create_cosmos_container(db, "mycontainer", partition_key="sex")
#'
#' proc <- create_stored_procedure(cont, "myBulkUpload",
#'     body=readLines(system.file("srcjs/bulkUpload.js", package="AzureCosmosR")))
#'
#' list_stored_procedures(cont)
#'
#' sw_male <- dplyr::filter(dplyr::starwars, sex == "male")
#' call_stored_procedure(proc, parameters=list(sw_male))
#'
#' delete_stored_procedure(proc)
#'
#' }
#' @aliases cosmos_stored_procedure
#' @rdname cosmos_stored_procedure
#' @export
list_stored_procedures <- function(container, ...)
{
    UseMethod("list_stored_procedures")
}

#' @export
list_stored_procedures.cosmos_container <- function(container, ...)
{
    res <- do_cosmos_op(container, "sprocs", "sprocs", "", ...)
    atts <- if(inherits(res, "response"))
        process_cosmos_response(res)$StoredProcedures
    else unlist(lapply(process_cosmos_response(res), `[[`, "StoredProcedures"), recursive=FALSE)
    lapply(atts, as_stored_procedure, container=container)
}


#' @rdname cosmos_stored_procedure
#' @export
create_stored_procedure <- function(container, ...)
{
    UseMethod("create_stored_procedure")
}

#' @rdname cosmos_stored_procedure
#' @export
create_stored_procedure.cosmos_container <- function(container, name, body, ...)
{
    body <- list(id=name, body=paste0(body, collapse="\n"))
    res <- do_cosmos_op(container, "sprocs", "sprocs", "", body=body, encode="json", http_verb="POST", ...)
    sproc <- process_cosmos_response(res)
    invisible(as_stored_procedure(sproc, container))
}


#' @rdname cosmos_stored_procedure
#' @export
call_stored_procedure <- function(container, ...)
{
    UseMethod("call_stored_procedure")
}

#' @rdname cosmos_stored_procedure
#' @export
call_stored_procedure.cosmos_container <- function(container, name, parameters=list(), ...)
{
    path <- file.path("sprocs", name)
    res <- do_cosmos_op(container, path, "sprocs", path, body=parameters, encode="json", http_verb="POST", ...)
    process_cosmos_response(res)
}

#' @rdname cosmos_stored_procedure
#' @export
call_stored_procedure.cosmos_stored_procedure <- function(container, ...)
{
    call_stored_procedure(container$container, container$id, ...)
}


#' @rdname cosmos_stored_procedure
#' @export
replace_stored_procedure <- function(container, ...)
{
    UseMethod("replace_stored_procedure")
}

#' @rdname cosmos_stored_procedure
#' @export
replace_stored_procedure.cosmos_container <- function(container, name, body, ...)
{
    body <- list(id=name, body=body)
    path <- file.path("sprocs", name)
    res <- do_cosmos_op(container, path, "sprocs", path, body=body, encode="json", http_verb="PUT", ...)
    sproc <- process_cosmos_response(res)
    invisible(as_stored_procedure(sproc, container))
}

#' @rdname cosmos_stored_procedure
#' @export
replace_stored_procedure.cosmos_stored_procedure <- function(container, ...)
{
    replace_stored_procedure.cosmos_container(container$container, container$id, container$body, ...)
}


#' @rdname cosmos_stored_procedure
#' @export
delete_stored_procedure <- function(container, ...)
{
    UseMethod("delete_stored_procedure")
}

#' @rdname cosmos_stored_procedure
#' @export
delete_stored_procedure.cosmos_container <- function(container, name, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, name, "stored procedure"))
        return(invisible(NULL))

    path <- file.path("sprocs", name)
    res <- do_cosmos_op(container, path, "attachments", path, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res))
}

#' @rdname cosmos_stored_procedure
#' @export
delete_stored_procedure.cosmos_stored_procedure <- function(container, ...)
{
    delete_stored_procedure(container$container, container$id, ...)
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
