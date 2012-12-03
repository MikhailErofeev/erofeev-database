package ru.compscicenter.db.erofeev.communication;

import java.io.Serializable;

public final class Entity implements Serializable {

    public static final long serialVersionUID = 0;

    private long key;
    transient boolean needFlush;
    transient long lastActive;
    private Serializable data;

    public Entity(long key, Serializable data) {
        needFlush = false;
        this.key = key;
        this.data = data;
    }


    @Override
    public String toString() {
        return "{key: "+key+ ";data: " + data.toString() + "}";
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

    public boolean isNeedFlush() {
        return needFlush;
    }

    public void setNeedFlush(boolean needFlush) {
        this.needFlush = needFlush;
    }

    public long getLastActive() {
        return lastActive;
    }

    public void setLastActive(long lastActive) {
        this.lastActive = lastActive;
    }
}
