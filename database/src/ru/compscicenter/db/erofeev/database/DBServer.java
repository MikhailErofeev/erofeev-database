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
import java.util.LinkedList;
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
            FileStorage.init(dbname + "/shards/shard" + shardIndex + "/" + dbname + serverIndex);
        } catch (IOException e) {
            e.printStackTrace();
            String ex = SerializationStuff.getStringFromException(e);
            Logger.getLogger("").warning(ex);
            Node.sendActivateResult(false, ex, this.getClass().getSimpleName(), parentAddress);
            return;
        }
        node.sendTrueActiveateResult();

    }


    public static void main(String[] args) throws IOException {
        new DBServer(args[0], Integer.valueOf(args[1]),
                Integer.valueOf(args[2]), args[3], args[4]);
    }

    class DBHanlder extends AbstractHandler {
        private long getID(Request request) {
            return Long.valueOf(request.getParams().get("Id").get(0));
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
                if (e == null) {
                    return new Response(Response.Code.BAD_REQUEST, null);
                }
                instance.put(e);
                if (master) {
                    return HttpClient.sendRequest(slaverAddress, request);
                }
            } else if (type == Request.RequestType.DELETE) {
                if (request.getParams().containsKey("Aliquant")) {
                    int total = Integer.valueOf(request.getParams().get("Aliquant_total").get(0));
                    int i = Integer.valueOf(request.getParams().get("Aliquant_index").get(0));
                    instance.removeAliquants(i, total);
                    if (master) {
                        return HttpClient.sendRequest(slaverAddress, request);
                    }
                } else {
                    instance.delete(id);
                    if (master) {
                        return HttpClient.sendRequest(slaverAddress, request);
                    }
                }
            } else {
                if (request.getParams().containsKey("Aliquant")) {
                    Logger.getLogger("").info("start Aliquant");
                    int total = Integer.valueOf(request.getParams().get("Aliquant_total").get(0));
                    int i = Integer.valueOf(request.getParams().get("Aliquant_index").get(0));
                    LinkedList<Entity> res = instance.getAlliquants(i, total);
                    Logger.getLogger("").info(res.toString());
                    return new Response(Response.Code.OK, res);
                } else {
                    Entity res = instance.get(id);
                    if (res == null) {
                        return new Response(Response.Code.NOT_FOUND, null);
                    } else {
                        return new Response(Response.Code.OK, res.getData());
                    }
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