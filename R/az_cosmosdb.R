#' @export
az_cosmosdb <- R6::R6Class("az_cosmosdb", inherit=AzureRMR::az_resource,

public=list(

    list_keys=function(read_only=FALSE)
    {
        op <- if(read_only) "readonlykeys" else "listkeys"
        unlist(self$do_operation(op, http_verb="POST"))
    },

    regen_key=function(kind=c("primary", "secondary", "primaryReadonly", "secondaryReadonly"))
    {
        kind <- match.arg(kind)
        self$do_operation("regenerateKey", body=list(keyKind=kind), http_verb="POST")
        invisible(self)
    },

    get_endpoint=function(key=self$list_keys()[[1]], key_type=c("master", "resource"))
    {
        key_type <- match.arg(key_type)
        cosmos_endpoint(self$properties$documentEndpoint, key=key, key_type=key_type)
    }
))
