package ru.compscicenter.db.erofeev.communication;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
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

    private final String serverName;

    public AbstractHandler(String serverName) {
        this.serverName = serverName;
    }

    public static HttpServer createServer(int port, AbstractHandler handler) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 10);
        HttpContext context = server.createContext("/", handler);
        return server;
    }

    public static int findPort(int start) {
        HttpServer server;
        while (true) {
            try {
                server = HttpServer.create(new InetSocketAddress(start++), 10);
                break;
            } catch (IOException e) {
            }
        }
        return start;
    }


    private final Request getRequest(HttpExchange exc) throws IOException {
        int length = 0;
        length = Integer.parseInt(exc.getRequestHeaders().remove("Content-length").get(0));
        byte[] result = new byte[length];
        exc.getRequestBody().read(result);
        Request.RequestType type = Request.RequestType.getType(exc.getRequestMethod());
        Request r = new Request(type, SerializationStuff.getObject(result), exc.getRequestHeaders());
        return r;
    }


    @Override
    public final void handle(HttpExchange exc) throws IOException {
        try {
            Request request = getRequest(exc);
            Response response = performRequest(request);
            response.addParam("node", serverName);
            sendResponse(exc, response);
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
