package ru.compscicenter.db.erofeev;

import ru.compscicenter.db.erofeev.common.Launcher;
import ru.compscicenter.db.erofeev.common.Node;
import ru.compscicenter.db.erofeev.communication.AbstractHandler;
import ru.compscicenter.db.erofeev.communication.Request;
import ru.compscicenter.db.erofeev.communication.Response;
import ru.compscicenter.db.erofeev.communication.SerializationStuff;
import ru.compscicenter.db.erofeev.database.DBServer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Доступ к слейвам
 * Инициализируется шардом
 * Инициализирует слейвы
 * Принимает put только с мастера, get только с шарда
 * Внутри содержит in-memory кеш шарда
 */
public class Slaver {

    private Node node;
    private int slavesN;
    private String shardAddress;
    private Queue<Slave> slaveQueue; //читаем по очереди
    private List<Slave> slaveList; //пишем во все сразу
    private String dbname;
    private final int shardIndex;


    private void initSlave(int serverIndex) throws IOException {
        Launcher.startServer(DBServer.class, new String[]{dbname, String.valueOf(shardIndex), String.valueOf(serverIndex), node.getAddress(), "-"});
    }

    public Slaver(String name, int shardIndex, int slaves, String shardAddress) {
        this.dbname = name;
        this.shardIndex = shardIndex;
        this.shardAddress = shardAddress;
        slavesN = slaves;
        this.slaveQueue = new LinkedBlockingQueue<Slave>();
        this.slaveList = new LinkedList<Slave>();
        node = null;
        try {
            node = new Node(name, this.getClass().getSimpleName() + shardIndex, new SlaverHandler(), shardAddress);
            node.getHttpServer().start();

            Logger.getLogger("").log(Level.INFO, "start slaver. slaves = " + slaves);
            for (int i = 1; i <= slaves; i++) {
                Logger.getLogger("").log(Level.INFO, "start init slave = " + i);
                initSlave(i);
            }
            Thread.sleep(2000 * slaves);
            Logger.getLogger("").info("times out");
            node.sendTrueActiveateResult();
        } catch (Exception e) {
            String ex = SerializationStuff.getStringFromException(e);
            Node.sendActivateResult(false, ex, this.getClass().getSimpleName(), shardAddress);
            node.sendActivateResult(false, "time out at " + node.getServerName());
            return;
        }
        node.sendTrueActiveateResult();
    }

    class Slave {
        public String addr;
        public int actives;

        public Slave(String addr) {
            this.addr = addr;
            actives = 0;
        }
    }

    class SlaverHandler extends AbstractHandler {

        void innerMessagesPerform(Request request) {
            Logger.getLogger("").log(Level.INFO, "request " + request);
            String message = request.getParams().get("Innermessage").get(0);
            if ("activate_ok".equals(message)) {
                String server = request.getParams().get("Server").get(0);
                String data = (String) request.getData();
                if (server == null) {
                    String ex = SerializationStuff.getStringFromException(new NullPointerException("server == null"));
                    Slaver.this.node.sendActivateResult(false, ex);
                } else if (data == null) {
                    String ex = SerializationStuff.getStringFromException(new NullPointerException("data == null"));
                    Slaver.this.node.sendActivateResult(false, ex);
                } else {
                    Slave s = new Slave(data);
                    slaveList.add(s);
                    slaveQueue.add(s);
                    Logger.getLogger("").log(Level.INFO, "add slave " + s.addr);
                    if (slaveList.size() == slavesN) {
                        Logger.getLogger("").log(Level.INFO, "all slaves");
                        Slaver.this.node.sendTrueActiveateResult();
                    }
                }

            } else if ("activate_fail".equals(message)) {
                Slaver.this.node.sendActivateResult(false, request.getData());
            }
        }

        @Override
        public Response performRequest(Request request) {
            if (request.getParams().containsKey("Innermessage")) {
                return new Response(Response.Code.OK, null);
            } else {
                return new Response(Response.Code.METHOD_NOT_ALLOWED, "Это " + Slaver.this.node.getServerName() + ". доступ только для своих");
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            return;
        } else {
            new Slaver(args[0], Integer.valueOf(args[1]), Integer.valueOf(args[2]), args[3]);
        }
    }
}
