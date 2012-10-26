package ru.compscicenter.db.erofeev.balancer;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 10/23/12
 * Time: 5:36 PM
 */
public class Balancer implements HttpHandler {

    int port;
    String dbname;
    String path;

    public Balancer(String dbname, String path) throws InterruptedException {
        this.path = path;
        HttpServer server = null;
        int portStart = 2300;
        this.dbname = dbname;
        while (true) {
            try {
                server = HttpServer.create(new InetSocketAddress(++portStart), 10);
                break;
            } catch (IOException e) {

            }
        }
        port = portStart;
        HttpContext context = server.createContext("/", this);
        server.start();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println(Arrays.toString(args));
        if (args.length < 2) {
            return;
        } else {
            new Balancer(args[0], args[1]);
        }
    }

    @Override
    public void handle(HttpExchange exc) throws IOException {
        exc.sendResponseHeaders(200, 0);
        String message = "Balancer ok. Port " + port + ". Db " + dbname + ". path  " + path;
        exc.getResponseBody().write(message.getBytes());
        exc.close();

    }
}
