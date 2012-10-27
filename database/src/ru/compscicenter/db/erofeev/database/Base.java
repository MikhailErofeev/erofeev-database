package ru.compscicenter.db.erofeev.database;

import java.io.*;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Base {

    /*
     * @FIXME Поток автоматической записи и подчистки кэша. глючит, на потом.
     * class Cron implements Runnable { @Override public void run() { while
     * (true) { try { flush(); cleanStore(); Thread.sleep(1000); } catch
     * (InterruptedException ex) { } } } }
     */
    private Map<Long, Entity> store; //ids - сущность, временное хранилище перед flush'eм, кэш
    private Map<Long, Integer> idToIndex; //ids - положение в файле
    private File file;
    private String baseName;
    private boolean needFlush;
    private int lastBaseSize;
    private static final int CACHE_SIZE = 1000;

    private static Base instance;

    public static boolean init(String name) {
        if (instance == null) {
            instance = new Base(name);
            return true;
        } else {
            return false;
        }
    }

    public static Base getInstance() {
        return instance;
    }

    private Base(String baseName) {

        store = Collections.synchronizedMap(new LruCache<Long, Entity>(CACHE_SIZE));
        idToIndex = new ConcurrentHashMap<Long, Integer>();
        this.baseName = baseName;
        lastBaseSize = 0;
        file = new File(baseName + ".me");
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException ex) {
                Logger.getLogger(Base.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        //ExecutorService exec = Executors.newCachedThreadPool();
        //exec.execute(new Cron());
        restoreIndexes();
    }

    private synchronized void restoreIndexes() {
        File info = new File(baseName + "_ind.me");
        if (info.exists()) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(info));
                br.readLine();
                String s;
                String[] res;
                while ((s = br.readLine()) != null) {
                    res = s.split(" ");
                    idToIndex.put(new Long(res[0]), new Integer(res[1]));
                }
            } catch (Exception ex) {
                Logger.getLogger(Base.class.getName()).log(Level.SEVERE, null, ex);
            }
            lastBaseSize = idToIndex.size();
        }
    }

    private synchronized void flushIndexes() {
        try {
            File info = new File(baseName + "_ind.me");
            BufferedWriter bw = new BufferedWriter(new FileWriter(info));
            //pw.(new String(idToIndex.size()));
            for (Map.Entry<Long, Integer> e : idToIndex.entrySet()) {
                bw.newLine();
                bw.write(e.getKey() + " " + e.getValue());
            }
            bw.close();
        } catch (Exception ex) {
            Logger.getLogger(Base.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public synchronized void flush() {
        //@FIXME это попрежнему узкое место, файл переписывается полностью после каждого флуша.
        //решать надо обращением к определённым байтам по началу и концу, зачисткой байтов, дописыванием в конец
        if (!needFlush) {
            return;
        }
        ObjectOutputStream writeS = null;
        ObjectInputStream objectInputStream = null;
        File fileNew = new File(baseName + "_temp.me");
        Entity readed;
        Entity inMem;
        int inserted = 0;
        boolean ok = false;
        try {
            fileNew.createNewFile();
            writeS = new ObjectOutputStream(new FileOutputStream(fileNew));
            if (lastBaseSize != 0) {
                objectInputStream = new ObjectInputStream(new FileInputStream(file));
                for (int currObj = 0; currObj < this.lastBaseSize; currObj++) { //проходим по объектам файла
                    readed = (Entity) objectInputStream.readObject();
                    if (idToIndex.containsKey(readed.getKey())) { //если содержится в индексах, 
                        inMem = store.get(readed.getKey());
                        if (inMem != null && inMem.needFlush) { //то проверяем, изменился ли
                            writeS.writeObject(inMem);
                            readed = inMem;
                            inMem.needFlush = false;
                        } else { //если не изменился, то старый и пишем
                            writeS.writeObject(readed);
                        }
                        idToIndex.put(readed.getKey(), inserted++); //изменяем номер объекта в файле
                    }
                }
            }
            for (Entity e : store.values()) { //для всех объектов во временном хранилище
                if (e.needFlush) { //если их не было в базе (если бы были, они бы уже заслались)
                    idToIndex.put(e.getKey(), inserted++); //добавлям в индексы
                    writeS.writeObject(e); //и в базу
                    e.needFlush = false;
                }

            }
            ok = true;
        } catch (Exception ex) {
            Logger.getLogger(Base.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (writeS != null) {
                    writeS.close();
                    writeS = null;
                }
                if (objectInputStream != null) {
                    objectInputStream.close();
                    objectInputStream = null;
                }
                System.gc(); //без этого могли быть проблемы, судя по stack overflow
                if (ok) {
                    file.delete();
                    fileNew.renameTo(file);
                    flushIndexes();
                    needFlush = false;
                    lastBaseSize = inserted;
                }
            } catch (Exception ex) {
                Logger.getLogger(Base.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private Entity readFromFile(int pos) {
        ObjectInputStream ois = null;
        Entity rslt = null;
        try {
            ois = new ObjectInputStream(new FileInputStream(file));
            for (int i = 0; i < pos; i++) {
                ois.readObject();
            }
            rslt = (Entity) ois.readObject();
        } catch (Exception ex) {
            Logger.getLogger(Base.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                ois.close();
            } catch (IOException ex) {
                Logger.getLogger(Base.class.getName()).log(Level.SEVERE, null, ex);
            }
            return rslt;
        }
    }


    void put(Entity e) {
        if (idToIndex.containsKey(e.getKey())) {
            update(e);
        } else {
            create(e);
        }
    }

    private void create(Entity e) {
        e.needFlush = true;
        needFlush = true;
        e.lastActive = System.currentTimeMillis();
        store.put(e.getKey(), e);
    }

    Entity read(Long key) {
        Entity rslt = store.get(key);
        if (rslt != null) {
            return rslt;
        }
        Integer ind = idToIndex.get(key);
        if (ind != null) {
            rslt = readFromFile(ind);
        } else {
            return null;
        }
        if (rslt != null) {
            rslt.lastActive = System.currentTimeMillis();
        }
        return rslt;
    }

    private synchronized void update(Entity e) {
        Long key = e.getKey();
        e.lastActive = System.currentTimeMillis();
        e.needFlush = true;
        needFlush = true;
        store.put(key, e);
    }

    void delete(Long key) {

        if (key == null) {
            return;
        }
        if (store.containsKey(key)) {
            needFlush = true;
            store.remove(key);
        }
        if (idToIndex.containsKey(key)) {
            needFlush = true;
            idToIndex.remove(key);
        }
    }
}
