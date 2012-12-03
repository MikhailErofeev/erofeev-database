package ru.compscicenter.db.erofeev.common;

import java.util.LinkedList;
import java.util.List;

/**
 * User: erofeev
 * Date: 11/30/12
 * Time: 7:49 PM
 */
public class Stuff {


    public static List<Long> longsFromStrings(List<String> strings) {
        List<Long> longs = new LinkedList<>();
        for (String s : strings) {
            longs.add(Long.valueOf(s));
        }
        return longs;
    }
}
