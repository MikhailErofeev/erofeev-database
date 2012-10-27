package ru.compscicenter.db.erofeev;

import ru.compscicenter.db.erofeev.balancer.Balancer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Миша
 * Date: 27.10.12
 * Time: 21:15
 */
public class Launcher {

    private static final String PATH = "out/production/database";

    public static void startServer(Class className, String[] params) throws IOException {

        StringBuilder command = new StringBuilder();
        command.append("java -cp '" + PATH + "'");
        command.append(" " + className.getCanonicalName());
        for (String s : params) {
            command.append(" '" + s + "'");
        }

        ProcessBuilder pb = new ProcessBuilder("bash", "-c", command.toString());
        pb.start();
    }
}
