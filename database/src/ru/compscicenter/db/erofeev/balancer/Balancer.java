package ru.compscicenter.db.erofeev.balancer;

import ru.compscicenter.db.erofeev.common.Node;
import ru.compscicenter.db.erofeev.communication.AbstractHandler;
import ru.compscicenter.db.erofeev.communication.HttpClient;
import ru.compscicenter.db.erofeev.communication.Request;
import ru.compscicenter.db.erofeev.communication.Response;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 10/23/12
 * Time: 5:36 PM
 */
public class Balancer extends AbstractHandler {

    public Balancer(String dbname, int shardIndex, String routerAddress) throws InterruptedException, IOException {
        Node node = new Node(dbname, "balancer" + shardIndex, this);
        node.getHttpServer().start();
        Request request = new Request(Request.RequestType.GET, null);
        request.addParam("Innermessage", node.getAddress());
        HttpClient.sendRequest(routerAddress, request);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println(Arrays.toString(args));
        if (args.length < 3) {
            return;
        } else {
            new Balancer(args[0], Integer.valueOf(args[1]), args[2]);
        }
    }

    @Override
    public Response performRequest(Request request) {
        return new Response(Response.Code.OK, "hello world!");
    }
}
