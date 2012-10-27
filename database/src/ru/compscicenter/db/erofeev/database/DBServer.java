package ru.compscicenter.db.erofeev.database;

/**
 * Created with IntelliJ IDEA.
 * User: Mikhail Erofeev
 * Date: 19.10.12
 * Time: 19:51
 */

import com.sun.net.httpserver.HttpServer;
import ru.compscicenter.db.erofeev.communication.AbstractHandler;
import ru.compscicenter.db.erofeev.communication.Request;
import ru.compscicenter.db.erofeev.communication.Response;

import java.io.IOException;

public class DBServer extends AbstractHandler {


    public static void main(String[] args) throws IOException {
        String serverName = "database";

        System.out.println("Server stoped");
    }

    private long getID(Request request) {
        return Long.valueOf(request.getParams().get("id").get(0));
    }

    public Response callDB(Request.RequestType type, long id, Entity data) {
        Base instance = Base.getInstance();

        if (type == Request.RequestType.PUT) {
            instance.put(data);
        } else if (type == Request.RequestType.DELETE) {
            instance.delete(id);
        } else {
            Entity res = instance.read(id);
            if (res == null) {
                return new Response(Response.Code.NOT_FOUND, null);
            } else {
                return new Response(Response.Code.OK, res);
            }
        }
        return new Response(Response.Code.OK, null);
    }

    @Override
    public Response performRequest(Request request) {
        long id = -1;
        try {
            id = getID(request);
        } catch (Exception e) {
            return new Response(Response.Code.BAD_REQUEST, null);
        }
        Entity e = null;
        if (request.getData() != null) {
            e = new Entity(id, request.getData());
        }
        return callDB(request.getType(), id, e);

    }


}