package ru.compscicenter.db.erofeev;

import ru.compscicenter.db.erofeev.common.Launcher;
import ru.compscicenter.db.erofeev.common.Node;
import ru.compscicenter.db.erofeev.communication.*;
import ru.compscicenter.db.erofeev.database.Entity;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 10/23/12
 * Time: 4:26 PM
 */
public class Router {
    private int onInit = 0;
    private boolean addNewShard = false;

    void lauchShard(int i) throws IOException {
        onInit++;
        Launcher.startServer(Shard.class, new String[]{dbname, String.valueOf(i), address, String.valueOf(slavesPerShard)});
    }

    void addShard() throws IOException {
        File shard = new File("./" + dbname + "/shards/shard" + shards.size());
        shard.mkdirs();
        lauchShard(shards.size());
    }

    void continueAddShard() {
        int i = 0;
        for (String addr : shards) {
            Request request = new Request(Request.RequestType.GET, null);
            request.addParam("Id", "-1");
            request.addParam("In", "ok");
            request.addParam("Aliquant", "ok");
            request.addParam("Aliquant_index", i + "");
            request.addParam("Aliquant_total", (shards.size()) + "");
            Response response = HttpClient.sendRequest(addr, request);
            List<Entity> entityList = (List<Entity>) response.getData();
            putAll(entityList);
            request.setType(Request.RequestType.DELETE);
            HttpClient.sendRequest(addr, request);
            i++;
        }
    }

    void putAll(List<Entity> entityList) {
        Request r;
        r = new Request(Request.RequestType.PUT, null);
        r.addParam("In", "ok");
        for (Entity e : entityList) {
            r.setData(e.getData());
            r.getParams().remove("Id");
            r.addParam("Id", e.getKey() + "");
            HttpClient.sendRequest(getHasedAddress(e.getKey()), r);
        }
    }

    void initShards(int shards) throws IOException, InterruptedException {
        File shardsFolder = new File("./" + dbname);
        if (shardsFolder.exists()) {
            shardsFolder.delete();
        }
        for (int i = 0; i < shards; i++) {
            File shard = new File("./" + dbname + "/shards/shard" + i);
            shard.mkdirs();
        }
        this.shards = new LinkedList<>();
        for (int i = 0; i < shards; i++) {
            lauchShard(i);
            Thread.sleep(500);
        }
    }

    private List<String> shards;

    private String address;
    private Node node;
    private final String dbname;
    private final int slavesPerShard;

    public Router(String dbname, int shards, int slavesPerShard) throws IOException, InterruptedException {
        this.dbname = dbname;
        this.slavesPerShard = slavesPerShard;
        node = new Node(dbname, "router", new RouterHandler(), null);
        node.getHttpServer().start();
        this.address = node.getAddress();
        initShards(shards);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        File f = new File("logs");
        if (f.exists()) {
            removeDirectory(f);
        }
        f.mkdir();
        new Router("notebook", 2, 1);
    }

    private static boolean removeDirectory(File directory) {
        String[] list = directory.list();
        if (list != null) {
            for (int i = 0; i < list.length; i++) {
                File entry = new File(directory, list[i]);
                if (entry.isDirectory()) {
                    if (!removeDirectory(entry))
                        return false;
                } else {
                    if (!entry.delete())
                        return false;
                }
            }
        }
        return directory.delete();
    }

    String getHasedAddress(long id) {
        return shards.get((int) (id % shards.size()));
    }


    class RouterHandler extends AbstractHandler {
        @Override
        public Response performRequest(Request request) {
            request.addParam("In", "ok"); //пометка, что запрос пришёл от главного сервера
            if (request.getParams().containsKey("Innermessage")) {
                if (request.getParams().get("Innermessage").get(0).equals("activate_ok")) {
                    onInit--;
                    shards.add((String) request.getData());
                    Collections.sort(shards);
                    if (onInit == 0 && addNewShard) {
                        addNewShard = false;
                        continueAddShard();
                    }
                } else if (request.getParams().get("Innermessage").get(0).equals("isActual")) {
                    String shard = request.getParams().get("Address").get(0);
                    if (shards.contains(shard)) {
                        return new Response(Response.Code.OK, null);
                    } else {
                        return new Response(Response.Code.NOT_FOUND, null);
                    }
                } else {
                    Logger.getLogger("").warning("система не инициализировалась. " + request.getData());
                }
                return new Response(Response.Code.OK, null);
            } else if (request.getParams().containsKey("Command")) {
                String command = request.getParams().get("Command").get(0);
                if (command.equals("add_shard")) {
                    try {
                        addNewShard = true;
                        addShard();
                        return new Response(Response.Code.OK, null);
                    } catch (Exception e) {
                        return new Response(Response.Code.BAD_REQUEST, SerializationStuff.getStringFromException(e));
                    }
                } else if (command.equals("remove_shard")) {
                    return new Response(Response.Code.METHOD_NOT_ALLOWED, "Это " + node.getServerName() + ". Ф-ия пока не реализована");
                } else {
                    return new Response(Response.Code.FORBIDDEN, "Это " + Router.this.node.getServerName() + ". запрос непоятен. попробуйте использовать add_shard, remove_shard");
                }
            } else if (request.getParams().containsKey("Id")) {
                List<String> ids = request.getParams().get("Id");
                if (ids.size() > 1) {
                    return new Response(Response.Code.METHOD_NOT_ALLOWED, "Это " + Router.this.node.getServerName() + ". добавление пачками пока не работает");
                } else {
                    long id = Long.valueOf(ids.get(0));
                    String addr = getHasedAddress(id);
                    return HttpClient.sendRequest(addr, request);
                }
            } else {
                return new Response(Response.Code.FORBIDDEN, "Это " + Router.this.node.getServerName() + ". запрос непоятен. попробуйте добавить Id в header");
            }
        }

    }
}
