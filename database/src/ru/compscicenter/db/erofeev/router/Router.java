package ru.compscicenter.db.erofeev.router;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 10/23/12
 * Time: 4:26 PM
 */
public class Router {
    void initShards(String name, int shards) throws IOException, InterruptedException {
        File root = new File(".");
        File shardsFolder = new File("./" + name);
        if (shardsFolder.exists()) {
            shardsFolder.delete();
        }
        shardsFolder.mkdir();
        for (int i = 0; i < shards; i++) {
            File shard = new File("./" + name + "/shards/shard" + i);
            shard.mkdirs();
            System.out.println(shard.getCanonicalFile());
            String command = "java -cp \'out/production/database\' " +
                    "ru.compscicenter.db.erofeev.balancer.Balancer '" + name + "' '" + shard.getCanonicalFile() + "'";
            ProcessBuilder pb = new ProcessBuilder("bash", "-c", command);
            pb.start();
        }
    }

    public Router(String name, int shards) {
        try {
            initShards(name, shards);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException {
        String name = "notebook";
        int shards = 2;
        new Router(name, shards);
    }
}
