package ru.compscicenter.db.erofeev.router;

import com.sun.net.httpserver.HttpServer;
import ru.compscicenter.db.erofeev.Launcher;
import ru.compscicenter.db.erofeev.Node;
import ru.compscicenter.db.erofeev.balancer.Balancer;
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
    void initShards(String name, int shards) throws IOException, InterruptedException {
        File root = new File(".");
        File shardsFolder = new File("./" + name);
        if (shardsFolder.exists()) {
            shardsFolder.delete();
        }
        for (int i = 0; i < shards; i++) {
            File shard = new File("./" + name + "/shards/shard" + i);
            shard.mkdirs();
        }
    }


    private String address;

    public static void main(String[] args) throws IOException, InterruptedException {
        String name = "notebook";
        int shards = 2;
        Router router = new Router();
        Node server = new Node(name, "router", router);
        System.out.println("Start router at " + server.getAddress());
        server.getHttpServer().start();
        router.initShards(name, shards);
        for (int i = 0; i < shards; i++) {
            Launcher.startServer(Balancer.class, new String[]{name, String.valueOf(i), server.getAddress()});
        }
    }


    @Override
    public Response performRequest(Request request) {
        if (request.getParams().containsKey("innerMessage")) {
            String message = request.getParams().get("innerMessage").get(0);
            System.out.println(message);
            return new Response(Response.Code.OK, null);
        } else {
            return new Response(Response.Code.METHOD_NOT_ALLOWED, "асанта сана бум-бум банана");
        }
    }
}
