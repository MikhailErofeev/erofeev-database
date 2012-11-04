package ru.compscicenter.db.erofeev;

import ru.compscicenter.db.erofeev.common.AbstractCron;
import ru.compscicenter.db.erofeev.common.Launcher;
import ru.compscicenter.db.erofeev.common.Node;
import ru.compscicenter.db.erofeev.communication.*;
import ru.compscicenter.db.erofeev.database.DBServer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 10/23/12
 * Time: 5:36 PM
 */
public class Shard {

    private static final int ACTUAL_CHECK_FREQ = 100 * 1000;
    private String slaverAddress;
    private String masterAddress;
    private String dbname;
    private int shardIndex;
    private int slaves;
    private Node node;
    private final String routerAddress;

    class ActualCron extends AbstractCron {

        protected ActualCron(long ms) {
            super(ms);
        }

        @Override
        protected void action() {
            Request request = new Request(Request.RequestType.GET, null);
            request.addParam("Innermessage", "isActual");
            request.addParam("Address", node.getAddress());
            Response response = HttpClient.sendRequest(routerAddress, request);
            if (response == null || response.getCode() != Response.Code.OK) {
                Request exitRequest = new Request(Request.RequestType.GET, null);
                exitRequest.addParam("Innermessage", "exit");
                exitRequest.addParam("In", "ok");
                exit(exitRequest);
            }

        }
    }


    private void initSlaver() throws IOException {
        Launcher.startServer(Slaver.class, new String[]{dbname, String.valueOf(shardIndex), String.valueOf(slaves), node.getAddress()});
    }

    private void initMaster() throws IOException {
        Launcher.startServer(DBServer.class, new String[]{dbname, String.valueOf(shardIndex), String.valueOf(0), slaverAddress, node.getAddress()});
    }

    public Shard(String dbname, int shardIndex, String routerAddress, int slaves) {
        this.slaves = slaves;
        this.dbname = dbname;
        this.shardIndex = shardIndex;
        this.routerAddress = routerAddress;
        try {
            node = new Node(dbname, this.getClass().getSimpleName() + shardIndex, new ShardHandler(), routerAddress);
            node.getHttpServer().start();
            initSlaver();
            Thread.sleep(2000 * (slaves + 3));
            node.sendActivateResult(false, "time out at " + node.getServerName());
        } catch (Exception e) {
            String ex = SerializationStuff.getStringFromException(e);
            Node.sendActivateResult(false, ex, this.getClass().getSimpleName(), routerAddress);
        }

    }

    public static void main(String[] args) {
        if (args.length < 4) {
            return;
        } else {
            new Shard(args[0], Integer.valueOf(args[1]), args[2], Integer.valueOf(args[3]));
        }
    }

    private void exit(Request request) {
        HttpClient.sendRequest(slaverAddress, request);
        HttpClient.sendRequest(masterAddress, request);
        node.getHttpServer().stop(23);
        Logger.getLogger("").info("exit");
        System.exit(23);
    }

    class ShardHandler extends AbstractHandler {

        void innerMessagesPerform(Request request) {
            String message = request.getParams().get("Innermessage").get(0);
            if ("activate_ok".equals(message)) {
                String server = request.getParams().get("Server").get(0);
                String data = (String) request.getData();
                if (server == null) {
                    String ex = SerializationStuff.getStringFromException(new NullPointerException("server == null"));
                    node.sendActivateResult(false, ex);
                } else if (data == null) {
                    String ex = SerializationStuff.getStringFromException(new NullPointerException("data == null"));
                    node.sendActivateResult(false, ex);
                } else if (server.startsWith("Slaver")) {
                    slaverAddress = data;
                    try {
                        initMaster();
                    } catch (Exception e) {
                        node.sendActivateResult(false, SerializationStuff.getStringFromException(e));
                    }
                } else if (server.startsWith("Master")) {
                    masterAddress = data;
                    node.sendTrueActiveateResult();
                    ExecutorService exec = Executors.newCachedThreadPool();
                    exec.execute(new ActualCron(ACTUAL_CHECK_FREQ));
                }
            } else if ("activate_fail".equals(message)) {
                node.sendActivateResult(false, request.getData());
            } else if ("exit".equals(message)) {
                exit(request);
            }
        }

        @Override
        public Response performRequest(Request request) {
            if (request.getParams().containsKey("Innermessage")) {
                innerMessagesPerform(request);
                return new Response(Response.Code.OK, null);
            } else if (!request.getParams().containsKey("In")) {
                return new Response(Response.Code.METHOD_NOT_ALLOWED, "Это " + node.getServerName() + ". доступ только для своих");
            } else if (request.getParams().containsKey("Id")) {
                if (request.getType() == Request.RequestType.GET) {
                    if (request.getParams().containsKey("Aliquant")) {
                        return HttpClient.sendRequest(masterAddress, request);
                    } else {
                        return HttpClient.sendRequest(slaverAddress, request);
                    }
                } else {
                    return HttpClient.sendRequest(masterAddress, request);
                }
            } else {
                return new Response(Response.Code.FORBIDDEN, "Это " + node.getServerName() + ". запрос непоятен. роутер, ты чего творишь?");
            }
        }
    }
}
