package ru.compscicenter.db.erofeev.database;

/**
 * Created with IntelliJ IDEA.
 * User: Mikhail Erofeev
 * Date: 19.10.12
 * Time: 19:51
 */

import ru.compscicenter.db.erofeev.common.Node;
import ru.compscicenter.db.erofeev.communication.*;

import java.io.IOException;
import java.util.logging.Logger;

public class DBServer {
    Node node;

    private boolean master;
    private String slaverAddress;

    public DBServer(String dbname, int shardIndex, int serverIndex, String slaverAddress, String shardAddress) {
        master = serverIndex == 0;
        String parentAddress = master ? shardAddress : slaverAddress;
        this.slaverAddress = slaverAddress;
        try {
            String indexes = shardIndex + "_" + serverIndex;
            node = new Node(dbname,
                    master ? "Master_" + indexes : "Slave_" + indexes, new DBHanlder(), parentAddress);
            Logger.getLogger("").info("master = " + master + ". parent = " + parentAddress);
            node.getHttpServer().start();
        } catch (IOException e) {
            e.printStackTrace();
            String ex = SerializationStuff.getStringFromException(e);
            Logger.getLogger("").warning(ex);
            Node.sendActivateResult(false, ex, this.getClass().getSimpleName(), parentAddress);
            return;
        }
        node.sendTrueActiveateResult();
        Logger.getLogger("").info("true result sended");

    }


    public static void main(String[] args) throws IOException {
        new DBServer(args[0], Integer.valueOf(args[1]),
                Integer.valueOf(args[2]), args[3], args[4]);
    }

    class DBHanlder extends AbstractHandler {
        private long getID(Request request) {
            return Long.valueOf(request.getParams().get("id").get(0));
        }

        private Response callDB(Request request) {
            FileStorage instance = FileStorage.getInstance();
            long id = -1;
            try {
                id = getID(request);
            } catch (Exception e) {
                return new Response(Response.Code.BAD_REQUEST, null);
            }
            Request.RequestType type = request.getType();
            Entity e = null;
            if (request.getData() != null) {
                e = new Entity(id, request.getData());
            }
            if (type == Request.RequestType.PUT) {
                instance.put(e);
                if (master) {
                    HttpClient.sendRequest(slaverAddress, request);
                }
            } else if (type == Request.RequestType.DELETE) {
                instance.delete(id);
                if (master) {
                    HttpClient.sendRequest(slaverAddress, request);
                }
            } else {
                Entity res = instance.get(id);
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
            if (request.getParams().containsKey("Innermessage")) {
                return new Response(Response.Code.OK, null);
            } else if (!request.getParams().containsKey("In")) {
                return new Response(Response.Code.METHOD_NOT_ALLOWED, "Это " + node.getServerName() + ". доступ только для своих");
            } else if (request.getParams().containsKey("Id")) {
                return callDB(request);
            } else {
                return new Response(Response.Code.FORBIDDEN, "Это " + node.getServerName() + ". запрос непоятен. роутер, ты чего творишь?");
            }

        }

    }

}