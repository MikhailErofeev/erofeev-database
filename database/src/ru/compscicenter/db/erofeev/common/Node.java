package ru.compscicenter.db.erofeev.common;

import com.sun.net.httpserver.HttpServer;
import ru.compscicenter.db.erofeev.communication.AbstractHandler;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 10/27/12
 * Time: 1:58 PM
 */


public class Node {

    public static HttpServer allocateServer(int portStart) {
        HttpServer server;
        while (true) {
            try {
                server = HttpServer.create(new InetSocketAddress(portStart), 10);
                return server;
            } catch (IOException e) {
            }
            portStart++;
        }
    }

    private HttpServer httpServer;
    private String serverName;
    private String address;

    public Node(String DBName, String role, AbstractHandler ah) throws IOException {
        httpServer = create();
        address = "http://localhost" + ":" + httpServer.getAddress().getPort();
        serverName = DBName + "_" + role.toUpperCase() + "_" + address;
        ah.setServerName(serverName);
        httpServer.createContext("/", ah);
    }

    public HttpServer getHttpServer() {
        return httpServer;
    }

    public void setHttpServer(HttpServer httpServer) {
        this.httpServer = httpServer;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    private static HttpServer create() throws IOException {
        HttpServer server = allocateServer(2300);
        return server;
    }

    /**
     * @param args - параметры развёртываемого сервера
     *             args[0] - имя базы данных
     *             args[1] - папка с классами, которые нужно запускать
     *             args[2] - адресс сервера-родителя
     */
    public final static void main(String[] args) {
        if (args.length < 2) {
            return;
        }
        String DBName = args[0];
        String classes = args[1];
        String parent = null;
        if (args.length > 2) {
            parent = args[3];
        }
        /*
        нужно поднять себя
        вызвать последующие сервера
        послать ОК родителю
         */

    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
