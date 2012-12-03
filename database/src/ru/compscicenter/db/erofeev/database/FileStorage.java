package ru.compscicenter.db.erofeev.database;

import ru.compscicenter.db.erofeev.common.AbstractCron;
import ru.compscicenter.db.erofeev.communication.Entity;

import java.io.*;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileStorage {

    //@FIXME Поток автоматической записи и подчистки кэша. глючит, на потом.
    class FlushCron extends AbstractCron {

        FlushCron(long ms) {
            super(ms);
        }

        @Override
        protected void action() {
            flush();
        }
    }

    private final Map<Long, Entity> store; //ids - сущность, временное хранилище перед flush'eм, кэш
    private final Map<Long, Integer> idToIndex; //ids - положение в файле
    private final File file;
    private final String baseName;
    private boolean needFlush;
    private int lastBaseSize;
    private static final int CACHE_SIZE = 100;
    //такой кэш спасёт от перезаписи часто изменяемых объектов, но не потребует много оперативной памяти

    private static FileStorage instance;

    public static boolean init(String name) {
        if (instance == null) {
            instance = new FileStorage(name);
            return true;
        } else {
            return false;
        }
    }


    public LinkedList<Entity> getAlliquants(int i, int total) {
        LinkedList<Entity> alliquans = new LinkedList<>();
        for (Map.Entry<Long, Integer> e : idToIndex.entrySet()) {
            if ((int) (e.getKey() % total) != i) {
                alliquans.add(get(e.getKey()));
            }
        }
        return alliquans;
    }

    public void removeAliquants(int i, int total) {
        for (Map.Entry<Long, Integer> e : idToIndex.entrySet()) {
            if ((int) (e.getKey() % total) != i) {
                delete(e.getKey());
            }
        }
    }

    public static FileStorage getInstance() {
        return instance;
    }

    private FileStorage(String baseName) {

        store = Collections.synchronizedMap(new LruCache(CACHE_SIZE));
        idToIndex = new ConcurrentHashMap<>();
        this.baseName = baseName;
        lastBaseSize = 0;
        file = new File(baseName + ".me");
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException ex) {
                Logger.getLogger(FileStorage.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        ExecutorService exec = Executors.newCachedThreadPool();
        exec.execute(new FlushCron(500));
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
                Logger.getLogger(FileStorage.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(FileStorage.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    //@FIXME нужно разбить на подфункции
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
                        if (inMem != null && inMem.isNeedFlush()) { //то проверяем, изменился ли
                            writeS.writeObject(inMem);
                            readed = inMem;
                            inMem.setNeedFlush(false);
                        } else { //если не изменился, то старый и пишем
                            writeS.writeObject(readed);
                        }
                        idToIndex.put(readed.getKey(), inserted++); //изменяем номер объекта в файле
                    }
                }
            }
            for (Entity e : store.values()) { //для всех объектов во временном хранилище
                if (e.isNeedFlush()) { //если их не было в базе (если бы были, они бы уже заслались)
                    idToIndex.put(e.getKey(), inserted++); //добавлям в индексы
                    writeS.writeObject(e); //и в базу
                    e.setNeedFlush(false);
                }

            }
            ok = true;
        } catch (Exception ex) {
            Logger.getLogger(FileStorage.class.getName()).log(Level.SEVERE, null, ex);
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
                Logger.getLogger(FileStorage.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(FileStorage.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (ois != null) {
                    ois.close();
                }
            } catch (IOException ex) {
                Logger.getLogger(FileStorage.class.getName()).log(Level.SEVERE, null, ex);
            }
            return rslt;
        }
    }


    public void put(Entity e) {
        if (idToIndex.containsKey(e.getKey())) {
            update(e);
        } else {
            create(e);
        }
    }

    private void create(Entity e) {
        e.setNeedFlush(true);
        needFlush = true;
        e.setLastActive(System.currentTimeMillis());
        store.put(e.getKey(), e);
    }

    public Entity get(Long key) {
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
            rslt.setLastActive(System.currentTimeMillis());
        }
        return rslt;
    }

    private synchronized void update(Entity e) {
        Long key = e.getKey();
        e.setLastActive(System.currentTimeMillis());
        e.setNeedFlush(true);
        needFlush = true;
        store.put(key, e);
    }

    public void delete(Long key) {

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
