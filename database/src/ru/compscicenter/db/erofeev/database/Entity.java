package ru.compscicenter.db.erofeev.database;

import java.io.Serializable;

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
    public String toString() {
        return this.data.toString();
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
