package ru.compscicenter.db.erofeev;

import ru.compscicenter.db.erofeev.common.Launcher;
import ru.compscicenter.db.erofeev.common.Node;
import ru.compscicenter.db.erofeev.communication.AbstractHandler;
import ru.compscicenter.db.erofeev.communication.Request;
import ru.compscicenter.db.erofeev.communication.Response;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

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
            if (request.getParams().containsKey("Innermessage")) {
                String message = request.getParams().get("Innermessage").get(0);
                System.out.println(message);
                System.out.println(request.getData());
                shards.add(message);
                return new Response(Response.Code.OK, null);
            } else {
                return new Response(Response.Code.METHOD_NOT_ALLOWED, "Это " + Router.this.node.getServerName() + ". доступ только для своих");
            }
        }
    }
}
