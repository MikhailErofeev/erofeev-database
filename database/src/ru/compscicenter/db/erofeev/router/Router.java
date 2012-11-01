package ru.compscicenter.db.erofeev.router;

import ru.compscicenter.db.erofeev.balancer.Balancer;
import ru.compscicenter.db.erofeev.common.Launcher;
import ru.compscicenter.db.erofeev.common.Node;
import ru.compscicenter.db.erofeev.communication.AbstractHandler;
import ru.compscicenter.db.erofeev.communication.Request;
import ru.compscicenter.db.erofeev.communication.Response;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 10/23/12
 * Time: 4:26 PM
 */
public class Router extends AbstractHandler {
    void initShards(String name, int shards, String address) throws IOException, InterruptedException {
        File root = new File(".");
        File shardsFolder = new File("./" + name);
        if (shardsFolder.exists()) {
            shardsFolder.delete();
        }
        for (int i = 0; i < shards; i++) {
            File shard = new File("./" + name + "/shards/shard" + i);
            shard.mkdirs();
        }
        for (int i = 0; i < shards; i++) {
            Launcher.startServer(Balancer.class, new String[]{name, String.valueOf(i), address});
            //Thread.sleep(1000);
        }
    }


    private String address;

    public static void main(String[] args) throws IOException, InterruptedException {
        String name = "notebook";
        int shards = 2;
        Router router = new Router();
        Node server = new Node(name, "router", router);
        server.getHttpServer().start();
        router.initShards(name, shards, server.getAddress());

        System.in.read();
    }


    @Override
    public Response performRequest(Request request) {
        if (request.getParams().containsKey("Innermessage")) {
            String message = request.getParams().get("Innermessage").get(0);
            System.out.println(message);
            return new Response(Response.Code.OK, null);
        } else {
            return new Response(Response.Code.METHOD_NOT_ALLOWED, "асанта сана бум-бум банана");
        }
    }
}
