package ru.compscicenter.db.erofeev;

import ru.compscicenter.db.erofeev.common.Launcher;
import ru.compscicenter.db.erofeev.common.Node;
import ru.compscicenter.db.erofeev.communication.*;
import ru.compscicenter.db.erofeev.database.DBServer;
import ru.compscicenter.db.erofeev.database.Entity;
import ru.compscicenter.db.erofeev.database.LruCache;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private final int slavesN;
    private Queue<Slave> slaveQueue; //читаем по очереди
    private final List<Slave> slaveList; //пишем во все сразу
    private String dbname;
    private final int shardIndex;
    private final Map<Long, Entity> cache;
    private ExecutorService exec = Executors.newCachedThreadPool();


    private void initSlave(int serverIndex) throws IOException {
        Launcher.startServer(DBServer.class, new String[]{dbname, String.valueOf(shardIndex), String.valueOf(serverIndex), node.getAddress(), "-"});
    }

    private Slaver(String name, int shardIndex, int slaves, String shardAddress) {
        this.dbname = name;
        this.shardIndex = shardIndex;
        slavesN = slaves;
        this.slaveQueue = new LinkedBlockingQueue<>();
        this.slaveList = new LinkedList<>();
        int CACHE_SIZE = 10000;
        cache = Collections.synchronizedMap(new LruCache(CACHE_SIZE));
        node = null;
        try {
            node = new Node(name, this.getClass().getSimpleName() + shardIndex, new SlaverHandler(), shardAddress);
            node.getHttpServer().start();

            Logger.getLogger("").log(Level.INFO, "try tostart slaver. slaves = " + slaves);
            for (int i = 1; i <= slaves; i++) {
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
        public final String addr;
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
            } else if ("exit".equals(message)) {
                for (Slave s : slaveList) {
                    HttpClient.sendRequest(s.addr, request);
                }
                node.getHttpServer().stop(23);
                Logger.getLogger("").info("exit");
                System.exit(23);
            }
        }

        private synchronized Slave getNextSlave() {
            Slave s = slaveQueue.poll();
            slaveQueue.offer(s);
            return s;
        }

        private Response readFromSlave(Request request) {
            Slave s = getNextSlave();
            return HttpClient.sendRequest(s.addr, request);
        }

        private Response getter(Request request) {
            long id = LongsFromStrings(request.getParams().get("Id")).get(0);
            Entity e = Slaver.this.cache.get(id);
            Response response;
            if (e == null) {
                response = readFromSlave(request);
                if (response.getCode() == Response.Code.OK) {
                    e = new Entity(id, response.getData());
                    Slaver.this.cache.put(id, e);
                }

                return response;
            } else {
                return new Response(Response.Code.OK, e.getData());
            }
        }

        private void asyncRequestToSlaves(Request request) {
            for (Slave s : slaveList) {
                exec.execute(new RequestExecutor(s.addr, request));
            }
        }

        private Response putter(Request request) {
            long id = LongsFromStrings(request.getParams().get("Id")).get(0);
            cache.put(id, new Entity(id, request.getData()));
            asyncRequestToSlaves(request);
            return new Response(Response.Code.OK, null);
        }

        private Response deleter(Request request) {
            long id = LongsFromStrings(request.getParams().get("Id")).get(0);
            if (id != -1) {
                cache.remove(id);
            }
            asyncRequestToSlaves(request);
            return new Response(Response.Code.OK, null);
        }

        private class RequestExecutor extends HttpClient.AsyncHttpClient {

            private RequestExecutor(String addr, Request request) {
                super(addr, request);
            }

            @Override
            public void performResponse(Response response) {
                //@TODO что если что-то пошло не так?
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
                    return getter(request);
                } else if (request.getType() == Request.RequestType.PUT) {
                    return putter(request);
                } else {
                    return deleter(request);
                }
            } else {
                return new Response(Response.Code.FORBIDDEN, "Это " + node.getServerName() + ". запрос непоятен. роутер, ты чего творишь?");
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
