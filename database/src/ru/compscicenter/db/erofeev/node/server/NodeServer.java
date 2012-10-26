package ru.compscicenter.db.erofeev.node.server;

/**
 * Created with IntelliJ IDEA.
 * User: Mikhail Erofeev
 * Date: 19.10.12
 * Time: 19:51
 */

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import ru.compscicenter.db.erofeev.communication.Response;
import ru.compscicenter.db.erofeev.node.base.Base;
import ru.compscicenter.db.erofeev.node.base.Entity;
import ru.compscicenter.db.erofeev.communication.Request;
import ru.compscicenter.db.erofeev.communication.RequestType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public class NodeServer implements HttpHandler {


    public static void requests() throws IOException {
        /*byte[] response = new Request(RequestType.put, 11, "KNOCK").send("localhost", 2300);
        byte[] response2 = new Request(RequestType.put, 22, "TUCK").send("localhost", 2300);
        byte[] response3 = new Request(RequestType.put, 33, "FUCK").send("localhost", 2300);
        byte[] response4 = new Request(RequestType.put, 120, new Contact("name", "sur")).send("localhost", 2300);*/
        Response resp = new Request(RequestType.get, 120, null).send("localhost", 2300);
        if (resp.getCode() == 200){
            System.out.println(resp.toString());
        }
        //


    }

    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(2300), 10);
        HttpContext context = server.createContext("/", new NodeServer());
        Base.init("test1");
        server.start();
        System.out.println("Server started\nPress any key to stop...");
        requests();
        //   System.in.read();
        server.stop(0);
        System.out.println("Server stoped");
    }

    @Override
    public void handle(HttpExchange exc) {
        try {

                Entity e = Base.getInstance().processRequest(r);
                exc.getResponseHeaders().add("test", "test");
                System.out.println("size " + exc.getResponseHeaders().size());

                if (r.type == RequestType.get) {
                    try {
                        System.out.println("here we are!");
                        if (e == null) {
                            exc.sendResponseHeaders(404, 0);
                            exc.getResponseBody().close();
                            exc.close();
                            return;
                        }
                        byte[] data = Request.getBytes(e.getData());
                        exc.getResponseHeaders().add("Content-Length", data.length + "");
                        exc.sendResponseHeaders(200, 0);
                        exc.getResponseBody().write(data);
                        exc.getResponseBody().flush();
                        System.out.println("ok1");
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                } else {
                    exc.sendResponseHeaders(200, 0);
                }
                Base.getInstance().flush();
            } else {
                exc.getResponseBody().write(new String("under construction").getBytes());
            }
            exc.getResponseBody().close();
            exc.close();
            System.out.println("ok2");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}