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
import java.util.Random;
import java.util.logging.Logger;

public class DBServer {
    private Node node;

    private boolean master;

    public DBServer(String dbname, int shardID, String router, boolean isMaster) {
        master = isMaster;
        try {
            node = new Node(dbname,
                    master ? "Master_" + shardID : "Slave_" + shardID,
                    new DBHanlder(), router);
            Logger.getLogger("").info("master = " + master + ". parent = " + router);
            node.getHttpServer().start();
            FileStorage.init(dbname + "/shards/shard" + shardID + "/" + new Random().nextInt() % 1000);
        } catch (IOException e) {
            e.printStackTrace();
            String ex = SerializationStuff.getStringFromException(e);
            Request request = new Request(Request.RequestType.PUT, ex);
            request.addParam("Innermessage", "fail");
            request.addParam("Shard", String.valueOf(shardID));
            HttpClient.sendRequest(router, request);
            System.exit(23);
            return;
        }
        Request request = new Request(Request.RequestType.PUT, node.getAddress());
        request.addParam("Innermessage", master ? "added_master" : "added_slave");
        request.addParam("Shard", String.valueOf(shardID));
        HttpClient.sendRequest(router, request);
        Logger.getLogger("").info("start OK");

    }


    public static void main(String[] args) throws IOException {
        new DBServer(args[0], Integer.valueOf(args[1]),
                args[2], Boolean.valueOf(args[3]));
    }

    class DBHanlder extends AbstractHandler {
        private long getID(Request request) {
            return Long.valueOf(request.getParams().get("Id").get(0));
        }

        private Response callDB(Request request) {
            FileStorage instance = FileStorage.getInstance();
            long id;
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
                if (e == null) {
                    return new Response(Response.Code.BAD_REQUEST, null);
                }
                instance.put(e);
            } else if (type == Request.RequestType.DELETE) {
                instance.delete(id);
            } else if (type == Request.RequestType.GET) {
                return new Response(Response.Code.OK, instance.get(id));
            }
            //@TODO перераспределние по шардам
            return new Response(Response.Code.OK, null);
        }

        @Override
        public Response performRequest(Request request) {
            if (request.getParams().containsKey("Innermessage")) {
                String inner = request.getParams().get("Innermessage").get(0);
                if ("exit".equals(inner)) {
                    node.getHttpServer().stop(23);
                    FileStorage.getInstance().flush();
                    Logger.getLogger("").info("exit");
                    System.exit(23);
                }
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