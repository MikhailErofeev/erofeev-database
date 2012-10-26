/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ru.compscicenter.db.erofeev.communication;

/**
 * @author Миша
 */
public enum RequestType {

    get, put, delete, undefined, info;

    static RequestType getType(String type) {
        for (RequestType rt : RequestType.values()) {
            if (rt.name().equalsIgnoreCase(type)) {
                return rt;
            }
        }
        return undefined;
    }
}
