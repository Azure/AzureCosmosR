// source: https://azurecosmosdb.github.io/labs/dotnet/labs/07-transactions-with-continuation.html
function bulkDelete(query) {
    var container = getContext().getCollection();
    var containerLink = container.getSelfLink();
    var response = getContext().getResponse();
    var responseBody = {
        deleted: 0,
        continuation: true
    };
    if (!query) throw new Error("The query is undefined or null.");
    tryQueryAndDelete();
    function tryQueryAndDelete(continuation) {
        var requestOptions = { continuation: continuation };
        var isAccepted = container.queryDocuments(
            containerLink,
            query,
            requestOptions,
            function (err, retrievedDocs, responseOptions) {
                if (err) throw err;
                if (retrievedDocs.length > 0) {
                    tryDelete(retrievedDocs);
                } else if (responseOptions.continuation) {
                    tryQueryAndDelete(responseOptions.continuation);
                } else {
                    responseBody.continuation = false;
                    response.setBody(responseBody);
                }
            }
        );
        if (!isAccepted) {
            response.setBody(responseBody);
        }
    }
    function tryDelete(documents) {
        if (documents.length > 0) {
            var isAccepted = container.deleteDocument(
                documents[0]._self,
                {},
                function (err, responseOptions) {
                    if (err) throw err;
                    responseBody.deleted++;
                    documents.shift();
                    tryDelete(documents);
                }
            );
            if (!isAccepted) {
                response.setBody(responseBody);
            }
        } else {
            tryQueryAndDelete();
        }
    }
}
