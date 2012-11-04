package ru.compscicenter.db.erofeev.common;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: Миша
 * Date: 27.10.12
 * Time: 21:15
 */
public class Launcher {

    private static final String PATH = "out/production/database";

    public static void startServer(Class className, String[] params) throws IOException{
        StringBuilder command = new StringBuilder();
        command.append("java -cp " + PATH + ":"+
                "database/lib/httpcomponents-client-4.2.1/lib/httpclient-4.2.1.jar:" +
                "database/lib/httpcomponents-client-4.2.1/lib/httpcore-4.2.1.jar:" +
                "database/lib/httpcomponents-client-4.2.1/lib/commons-logging-1.1.1.jar");
        //@FIXME кажется я выстрелил себе в ногу
        command.append(" " + className.getCanonicalName());
        for (String s : params) {
            command.append(" '" + s + "'");
        }
        System.out.println(command.toString());
        ProcessBuilder pb = new ProcessBuilder("bash", "-c", command.toString());
        pb.start();
    }
}
