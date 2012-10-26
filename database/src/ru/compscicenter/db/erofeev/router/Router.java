package ru.compscicenter.db.erofeev.router;

import com.sun.net.httpserver.HttpServer;
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
        shardsFolder.mkdir();
        for (int i = 0; i < shards; i++) {
            File shard = new File("./" + name + "/shards/shard" + i);
            shard.mkdirs();
            System.out.println(shard.getCanonicalFile());
            String command = "java -cp \'out/production/database\' " +
                    "ru.compscicenter.db.erofeev.balancer.Balancer '" + name + "' '" + shard.getCanonicalFile() + " " + address;
            ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
            pb.start();
        }
    }

    public Router(String name) {
        super("ROUTER" + name);

    }

    private String address;

    public static void main(String[] args) throws IOException, InterruptedException {
        String name = "notebook";
        int shards = 2;
        Router router = new Router(name);
        HttpServer httpServer = createServer(findPort(2300), router);
        router.address = httpServer.getAddress().getHostString() + ":" + httpServer.getAddress().getPort();
        router.initShards(name, shards);
        httpServer.start();
    }


    @Override
    public Response performRequest(Request request) {
        if (request.getParams().containsKey("innerMessage")) {
            String message = request.getParams().get("innerMessage").get(0);
            System.out.println(message);
            return new Response(Response.Code.OK, null);
        } else {
            return new Response(Response.Code.METHOD_NOT_ALLOWED, null);
        }
    }
}
