package ru.compscicenter.db.erofeev;

import ru.compscicenter.db.erofeev.common.Launcher;
import ru.compscicenter.db.erofeev.common.Stuff;
import ru.compscicenter.db.erofeev.communication.Entity;
import ru.compscicenter.db.erofeev.communication.HttpClient;
import ru.compscicenter.db.erofeev.communication.Request;
import ru.compscicenter.db.erofeev.communication.Response;
import ru.compscicenter.db.erofeev.database.DBServer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 10/23/12
 * Time: 5:36 PM
 */
public class Shard {

    private static final int ACTUAL_CHECK_FREQ = 100 * 1000;
    private String masterAddress;
    private String dbname;
    private int id;
    private int slaves;
    private Router router;
    private ExecutorService exec = Executors.newCachedThreadPool();

    private Queue<Slave> slaveQueue; //читаем по очереди
    private List<Slave> slaveList; //пишем во все сразу


    class Slave {
        public final String addr;
        public int actives;

        public Slave(String addr) {
            this.addr = addr;
            actives = 0;
        }
    }


    private void initMaster() throws IOException {
        Launcher.startServer(DBServer.class,
                new String[]{router.getDbname(), String.valueOf(id),
                        router.getNode().getAddress(), String.valueOf(true)});
    }

    private void initSlave() throws IOException {
        Launcher.startServer(DBServer.class,
                new String[]{router.getDbname(), String.valueOf(id),
                        router.getNode().getAddress(), String.valueOf(false)});
    }

    public Shard(Router router, int shardIndex) {
        this.router = router;
        this.id = shardIndex;
        slaveList = new LinkedList<>();
        slaveQueue = new LinkedBlockingQueue<>();
        activateSlaves();
        try {
            initMaster();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(shardIndex + " init");
    }

    private void activateSlaves() {
        try {
            for (int i = 1; i <= router.getSlavesPerShard(); i++) {
                initSlave();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getId() {
        return id;
    }

    void innerMessagesPerform(Request request) {
        String message = request.getParams().get("Innermessage").get(0);
        String data = (String) request.getData();
        if (data == null) {
            throw new NullPointerException("data == null");
        }
        if ("added_slave".equals(message)) {
            Slave s = new Slave(data);
            slaveList.add(s);
            slaveQueue.add(s);
            Logger.getLogger("").log(Level.INFO, "add slave " + s.addr);
            if (slaveList.size() == router.getSlavesPerShard()) {
                System.out.println("all slaves ok");
            }
        } else if ("added_master".equals(message)) {
            masterAddress = data;
        } else if (("fail").equals(message)) {
            System.err.println(data);
        } else {
            System.err.println("непоятное внутреннее сообщение: " + message);
        }
    }

    public Response performRequest(Request request) {
        if (request.getParams().containsKey("Id")) {
            if (request.getType() == Request.RequestType.GET) {
                return getter(request);
            } else if (request.getType() == Request.RequestType.PUT) {
                return putter(request);
            } else if (request.getType() == Request.RequestType.DELETE) {
                return deleter(request);
            } else {
                return new Response(Response.Code.METHOD_NOT_ALLOWED, "Это " + router.getNode().getServerName() + ". метод не доступен");
            }
        } else {
            return new Response(Response.Code.METHOD_NOT_ALLOWED, "Это " + router.getNode().getServerName() + ". ID не указан");
        }
    }

    private Response putter(Request request) {
        long id = Stuff.longsFromStrings(request.getParams().get("Id")).get(0);
        router.getCache().put(id, new Entity(id, request.getData()));
        asyncRequestToSlaves(request);
        return new Response(Response.Code.OK, null);
    }

    private Response deleter(Request request) {
        long id = Stuff.longsFromStrings(request.getParams().get("Id")).get(0);
        if (id != -1) {
            router.getCache().remove(id);
        }
        asyncRequestToSlaves(request);
        return new Response(Response.Code.OK, null);
    }

    private synchronized Slave getNextSlave() {
        Slave s = slaveQueue.poll();
        slaveQueue.offer(s);
        return s;
    }

    private void asyncRequestToSlaves(Request request) {
        for (Slave s : slaveList) {
            exec.execute(new RequestExecutor(s.addr, request));
        }
    }


    private Response readFromSlave(Request request) {
        Slave s = getNextSlave();
        return HttpClient.sendRequest(s.addr, request);
    }


    private Response getter(Request request) {
        long id = Stuff.longsFromStrings(request.getParams().get("Id")).get(0);
        Entity e = router.getCache().get(id);
        if (e == null) {
            Response response = readFromSlave(request);
            if (response.getCode() == Response.Code.OK) {
                e = new Entity(id, response.getData());
                router.getCache().put(id, e);
            }
            return response;
        } else {
            return new Response(Response.Code.OK, e.getData());
        }
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
}
