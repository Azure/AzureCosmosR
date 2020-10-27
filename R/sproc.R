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
        process_cosmos_response(res, ...)$StoredProcedures
    else unlist(lapply(process_cosmos_response(res, ...), `[[`, "StoredProcedures"), recursive=FALSE)
    lapply(atts, as_stored_procedure, container=container)
}


#' @export
create_stored_procedure <- function(container, ...)
{
    UseMethod("create_stored_procedure")
}

#' @export
create_stored_procedure.cosmos_container <- function(container, name, body, ...)
{
    body <- list(id=name, body=body)
    res <- do_cosmos_op(container, "sprocs", "sprocs", "", body=body, encode="json", http_verb="POST", ...)
    sproc <- process_cosmos_response(res, ...)
    invisible(as_stored_procedure(sproc, container))
}


#' @export
call_stored_procedure <- function(container, ...)
{
    UseMethod("call_stored_procedure")
}

#' @export
call_stored_procedure.cosmos_container <- function(container, name, parameters=list(), ...)
{
    path <- file.path("sprocs", name)
    res <- do_cosmos_op(container, path, "sprocs", path, body=parameters, encode="json", http_verb="POST", ...)
    process_cosmos_response(res, ...)
}

#' @export
call_stored_procedure.cosmos_stored_procedure <- function(container, ...)
{
    call_stored_procedure(container$container, container$id, ...)
}


#' @export
delete_stored_procedure <- function(container, ...)
{
    UseMethod("delete_stored_procedure")
}

#' @export
delete_stored_procedure.cosmos_container <- function(container, name, confirm=TRUE, ...)
{
    if(!delete_confirmed(confirm, id, "stored procedure"))
        return(invisible(NULL))

    path <- file.path("sprocs", name)
    res <- do_cosmos_op(container, path, "attachments", path, http_verb="DELETE", ...)
    invisible(process_cosmos_response(res, ...))
}

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
