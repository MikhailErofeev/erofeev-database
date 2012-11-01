package ru.compscicenter.db.erofeev.communication;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 10/26/12
 * Time: 10:05 AM
 */

//@TODO надо его обозвать получше
public abstract class AbstractHandler implements HttpHandler {

    //добавляет имя сервера в хедер, чтобы можно было отследить перемещение запроса
    private String serverName;

    public AbstractHandler() {
        serverName = "undefined";
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    private final Request getRequest(HttpExchange exc) throws IOException {
        int length = 0;
        if (exc.getRequestHeaders().get("Content-length") != null){
            length = Integer.parseInt(exc.getRequestHeaders().get("Content-length").get(0));
        }
        byte[] result = null;
        if (length > 0) {
            result = new byte[length];
            exc.getRequestBody().read(result);
        }
        Request.RequestType type = Request.RequestType.getType(exc.getRequestMethod());
        Request r = new Request(type, SerializationStuff.getObject(result), exc.getRequestHeaders());
        return r;
    }


    @Override
    public final void handle(HttpExchange exc) throws IOException {
        System.out.println("new request");
        try {
            Request request = getRequest(exc);
            System.out.println(request.toString());
            Response response = performRequest(request);
            response.addParam("node", serverName);
            System.out.println(response.toString());
            sendResponse(exc, response);
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } finally {
            exc.getResponseBody().flush();
            exc.getResponseBody().close();
            exc.close();
        }
    }

    private final void sendResponse(HttpExchange exc, Response response) throws IOException {
        for (Map.Entry<String, List<String>> entry : response.getParams().entrySet()) {
            for (String value : entry.getValue()) {
                exc.getResponseHeaders().add(entry.getKey(), value);
            }
        }
        byte[] data = null;
        long length = -1;
        if (response.getData() != null) {
            data = SerializationStuff.getBytes(response.getData());
            length = data.length;
        }
        exc.sendResponseHeaders(response.getCode().code, length);
        if (data != null) {
            exc.getResponseBody().write(data);
        }

    }

    public abstract Response performRequest(Request request);
}
