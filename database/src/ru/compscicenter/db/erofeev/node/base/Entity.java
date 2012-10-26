package ru.compscicenter.db.erofeev.node.base;

import java.io.*;

public final class Entity implements Serializable {

    public static final long serialVersionUID = 0;

    private long key;
    transient boolean needFlush;
    transient long lastActive;
    Serializable data;

    public Entity(long key, Serializable data) {
        needFlush = false;
        this.key = key;
        this.data = data;
    }



    @Override
    public String toString(){
        return this.data.toString();
    }

    public static byte[] translatePainTextBytes(byte[] src){
        ObjectInputStream readS = null;
        Object c;
          try{
              readS = new ObjectInputStream(new ByteArrayInputStream(src));
              c = (Object) readS.readObject();
          }catch (Exception e){
              e.printStackTrace();
              return src;
          }
        if (c instanceof byte[]){
            System.out.println("translate!");
            return new String((byte[])c).getBytes();
        }else{
            return src;
        }
    }

    public Serializable getData() {
        return data;
    }


    public long getKey() {
        return key;
    }


    public void setKey(long key) {
        this.key = key;
    }
}
