package ru.compscicenter.db.erofeev;

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
public abstract class AbstractServer extends AbstractHandler {

    private String serverName;
    private String DBName;

    public enum ServerRole {
        ROUTER, BALANCER, CACHE, MASTER, SLAVE;
    }

    public static int findPort(int start) {
        HttpServer server;
        while (true) {
            try {
                server = HttpServer.create(new InetSocketAddress(start++), 10);
                break;
            } catch (IOException e) {
            }
        }
        return start;
    }

    public AbstractServer(String DBName) {
        super(DBName);
        this.DBName = DBName;
    }

    private static HttpServer upServer() {
        int port = findPort(2300);
        HttpServer server = null;
        try {
            server = HttpServer.create(new InetSocketAddress(port), 10);
        } catch (IOException e) {
            return null;
        }

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
        HttpServer server = upServer();
        if (server == null){
            return null;
        }
        String address = server.getAddress().getHostString() + ":" + server.getAddress().getPort();
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
}
