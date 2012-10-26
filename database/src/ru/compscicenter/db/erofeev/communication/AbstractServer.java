package ru.compscicenter.db.erofeev.communication;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 10/26/12
 * Time: 10:05 AM
 */
public abstract class AbstractServer implements HttpHandler {


    Request getRequest(HttpExchange exc) throws IOException {
        int length = 0;
        length = Integer.parseInt(exc.getRequestHeaders().remove("Content-length").get(0));
        /*long[] ids = null;
        if (idParam != null) {
            if (idParam.charAt(0) == '[' && idParam.charAt(idParam.length() - 1) == ']') {
                idParam = idParam.substring(1, idParam.length() - 2);
            }
            int i = 0;
            String[] sids = idParam.split(",");
            ids = new long[sids.length];
            for (String sid : sids) {
                ids[i++] = Long.valueOf(sid);
            }
        } */
        byte[] result = new byte[length];
        exc.getRequestBody().read(result);
        RequestType type = RequestType.getType(exc.getRequestMethod());
        Request r = new Request(type, SerializationStuff.getObject(result), exc.getRequestHeaders());
        return r;
    }


    @Override
    public void handle(HttpExchange exc) throws IOException {
        Request r = getRequest(exc);

    }

    public abstract void performRequest(Request request);
}
