package ru.compscicenter.db.erofeev;

import ru.compscicenter.db.erofeev.common.Launcher;
import ru.compscicenter.db.erofeev.common.Node;
import ru.compscicenter.db.erofeev.communication.AbstractHandler;
import ru.compscicenter.db.erofeev.communication.HttpClient;
import ru.compscicenter.db.erofeev.communication.Request;
import ru.compscicenter.db.erofeev.communication.Response;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 10/23/12
 * Time: 4:26 PM
 */
public class Router {
    void initShards(String name, int shards, String address, int slaves) throws IOException, InterruptedException {
        File shardsFolder = new File("./" + name);
        if (shardsFolder.exists()) {
            shardsFolder.delete();
        }
        for (int i = 0; i < shards; i++) {
            File shard = new File("./" + name + "/shards/shard" + i);
            shard.mkdirs();
        }
        this.shards = new LinkedList<>();
        for (int i = 0; i < shards; i++) {
            Launcher.startServer(Shard.class, new String[]{name, String.valueOf(i), address, String.valueOf(slaves)});
        }
    }

    List<String> shards;

    private String address;
    private Node node;

    public Router(String dbname, int shards, int slavesPerShard) throws IOException, InterruptedException {
        node = new Node(dbname, "router", new RouterHandler(), null);
        node.getHttpServer().start();
        initShards(dbname, shards, node.getAddress(), slavesPerShard);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        new Router("notebook", 1, 1);
    }


    class RouterHandler extends AbstractHandler {
        @Override
        public Response performRequest(Request request) {
            request.addParam("In", "ok"); //пометка, что запрос пришёл от главного сервера
            if (request.getParams().containsKey("Innermessage")) {
                if (request.getParams().get("Innermessage").get(0).equals("activate_ok")) {
                    shards.add((String) request.getData());
                } else {
                    Logger.getLogger("").warning("система не инициализировалась. " + (String) request.getData());
                }
                return new Response(Response.Code.OK, null);
            } else if (request.getParams().containsKey("Id")) {
                List<String> ids = request.getParams().get("Id");
                if (ids.size() > 1) {
                    return new Response(Response.Code.METHOD_NOT_ALLOWED, "Это " + Router.this.node.getServerName() + ". добавление пачками пока не работает");
                } else {
                    long id = Long.valueOf(ids.get(0));
                    String addr = shards.get((int) (id % shards.size()));
                    return HttpClient.sendRequest(addr, request);
                }
            } else {
                return new Response(Response.Code.FORBIDDEN, "Это " + Router.this.node.getServerName() + ". запрос непоятен. попробуйте добавить Id в header");
            }
        }

    }
}
