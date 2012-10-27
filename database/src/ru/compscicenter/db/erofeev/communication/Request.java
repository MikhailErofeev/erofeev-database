package ru.compscicenter.db.erofeev.communication;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Request {

    private RequestType type;
    private Serializable data;
    private Map<String, List<String>> params;

    public enum RequestType {

        GET, PUT, DELETE;

        static RequestType getType(String type) {
            for (RequestType rt : RequestType.values()) {
                if (rt.name().equalsIgnoreCase(type)) {
                    return rt;
                }
            }
            throw new IllegalArgumentException(type);
        }
    }

    public Request(RequestType type, Serializable data) {
        this.type = type;
        this.data = data;
        this.params = new HashMap<String, List<String>>();
    }


    public Request(RequestType type, Serializable data, Map params) {
        this.type = type;
        this.data = data;
        this.params = params;
    }

    public void addParam(String key, String value) {
        if (key.equalsIgnoreCase("Content-Length")) {
            return;
        }
        if (!params.containsKey(key)) {
            params.put(key, new LinkedList<String>());
        }
        params.get(key).add(value);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Request ");
        sb.append(type.toString() + " | ");
        sb.append(data + " | ");
        sb.append(params);
        return sb.toString();
    }

    public RequestType getType() {
        return type;
    }

    public Serializable getData() {
        return data;
    }

    public Map<String, List<String>> getParams() {
        return params;
    }
}
