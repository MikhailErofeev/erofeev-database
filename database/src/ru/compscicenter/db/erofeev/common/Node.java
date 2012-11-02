package ru.compscicenter.db.erofeev.common;

import com.sun.net.httpserver.HttpServer;
import ru.compscicenter.db.erofeev.communication.AbstractHandler;
import ru.compscicenter.db.erofeev.communication.HttpClient;
import ru.compscicenter.db.erofeev.communication.Request;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    private AbstractHandler handler;
    private String parentAddress;
    private String role;

    public Node(String DBName, String role, AbstractHandler ah, String parentAddress) throws IOException {
        this.parentAddress = parentAddress;
        handler = ah;
        httpServer = create();
        //@FIXME получить нормальный адрес из httpServer неполучилось
        address = "http://localhost" + ":" + httpServer.getAddress().getPort();
        serverName = DBName + "_" + role + "_" + address;
        this.role = role;
        ah.setServerName(serverName);
        Handler handler = new FileHandler("logs/"+DBName + "_" + role + ".log");
        Logger.getLogger("").setLevel(Level.INFO);
        Logger.getLogger("").addHandler(handler);
        Logger.getLogger("").info("set logger for " + serverName);
        httpServer.createContext("/", ah);
    }

    public HttpServer getHttpServer() {
        return httpServer;
    }

    public String getServerName() {
        return serverName;
    }

    boolean isReady;

    //огромная конструкция на случай ошибки при инициализации Node
    private static synchronized void sendActivateResult(boolean result, Serializable message,
                                                        boolean isReady, String role, String parentAddress) {
        Request request = new Request(Request.RequestType.PUT, message);
        request.addParam("Innermessage", result ? "activate_ok" : "activate_fail");
        request.addParam("Server", role);
        Logger.getLogger("").info("send request " + request);
        System.out.println(HttpClient.sendRequest(parentAddress, request));
    }

    public static void sendActivateResult(boolean result, Serializable message, String role, String parentAddress) {
        sendActivateResult(result, message, false, role, parentAddress);
    }

    public void sendActivateResult(boolean result, Serializable message) {
        if (isReady) {
            return;
        }
        isReady = true;
        sendActivateResult(result, message, isReady, role, parentAddress);
    }

    public synchronized void sendTrueActiveateResult() {
        sendActivateResult(true, this.getAddress());
    }


    private static HttpServer create() throws IOException {
        HttpServer server = allocateServer(2300);
        return server;
    }

    public String getAddress() {
        return address;
    }

}
