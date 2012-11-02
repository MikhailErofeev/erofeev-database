package ru.compscicenter.db.erofeev;

import ru.compscicenter.db.erofeev.common.Launcher;
import ru.compscicenter.db.erofeev.common.Node;
import ru.compscicenter.db.erofeev.communication.AbstractHandler;
import ru.compscicenter.db.erofeev.communication.Request;
import ru.compscicenter.db.erofeev.communication.Response;
import ru.compscicenter.db.erofeev.communication.SerializationStuff;
import ru.compscicenter.db.erofeev.database.DBServer;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 10/23/12
 * Time: 5:36 PM
 */
public class Shard {

    private String slaverAddress;
    private String masterAddress;
    private String routerAddress;
    private String dbname;
    private int shardIndex;
    private int slaves;
    private Node node;

    boolean isReady;

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
        isReady = false;
        this.routerAddress = routerAddress;
        try {
            node = new Node(dbname, this.getClass().getSimpleName() + shardIndex, new ShardHandler(), routerAddress);
            node.getHttpServer().start();
            System.out.println("server init at " + node.getAddress());
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


    class ShardHandler extends AbstractHandler {

        void innerMessagesPerform(Request request) {
            String message = request.getParams().get("Innermessage").get(0);
            if ("activate_ok".equals(message)) {
                String server = request.getParams().get("Server").get(0);
                String data = (String) request.getData();
                if (server == null) {
                    String ex = SerializationStuff.getStringFromException(new NullPointerException("server == null"));
                    Shard.this.node.sendActivateResult(false, ex);
                } else if (data == null) {
                    String ex = SerializationStuff.getStringFromException(new NullPointerException("data == null"));
                    Shard.this.node.sendActivateResult(false, ex);
                } else if (server.startsWith("Slaver")) {
                    slaverAddress = data;
                    try {
                        initMaster();
                    } catch (Exception e) {
                        Shard.this.node.sendActivateResult(false, SerializationStuff.getStringFromException(e));
                    }
                } else if (server.startsWith("Master")) {
                    masterAddress = data;
                    Shard.this.node.sendTrueActiveateResult();
                }
            } else if ("activate_fail".equals(message)) {
                Shard.this.node.sendActivateResult(false, request.getData());
            }
        }

        @Override
        public Response performRequest(Request request) {
            if (request.getParams().containsKey("Innermessage")) {
                innerMessagesPerform(request);
                return new Response(Response.Code.OK, "ok ok");
            } else {
                return new Response(Response.Code.METHOD_NOT_ALLOWED, "Это " + Shard.this.node.getServerName() + ". доступ только для своих");
            }
        }
    }
}
