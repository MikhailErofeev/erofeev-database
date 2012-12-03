package ru.compscicenter.db.erofeev.database;

import ru.compscicenter.db.erofeev.communication.Entity;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Mikhail Erofeev
 * Date: 19.10.12
 * Time: 19:28
 */
public class LruCache extends LinkedHashMap<Long, Entity> {
    private final int maxEntries;

    public LruCache(final int maxEntries) {
        super(maxEntries + 1, 1.0f, true);
        this.maxEntries = maxEntries;
    }

    @Override
    protected boolean removeEldestEntry(final Map.Entry<Long, Entity> eldest) {
        if (eldest.getValue().isNeedFlush()) {
            return false;
        } else {
            return super.size() > maxEntries;
        }
    }
}

