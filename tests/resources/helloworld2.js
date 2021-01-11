function helloworld2(name) {
    var context = getContext();
    var response = context.getResponse();
    response.setBody("Hello, " + name);
}
