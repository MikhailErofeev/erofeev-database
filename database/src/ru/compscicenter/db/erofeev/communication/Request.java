package ru.compscicenter.db.erofeev.communication;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Request {

    private RequestType type;
    private Serializable data;
    private final Map<String, List<String>> params;

    public void setType(RequestType type) {
        this.type = type;
    }

    public void setData(Serializable data) {
        this.data = data;
    }

    public enum RequestType {

        GET, PUT, DELETE;

        static RequestType getType(String type) {
            if (type.equalsIgnoreCase("POST")) {
                return PUT;
            }
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
        this.params = new HashMap<>();
    }


    public Request(RequestType type, Serializable data, Map<String, List<String>> params) {
        this.type = type;
        this.data = data;
        this.params = new HashMap<>();

        for (Map.Entry<String, List<String>> entry : params.entrySet()) {
            this.params.put(entry.getKey(), entry.getValue());
        }
        this.params.remove("Content-length");
        this.params.remove("Accept-language");
        this.params.remove("Content-type");
        this.params.remove("Accept-encoding");
        this.params.remove("Accept-charset");
        this.params.remove("User-agent");
        this.params.remove("Accept");
        this.params.remove("Connection");
        this.params.remove("Host");
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
        sb.append(params + " | ");
        sb.append("Data: " + data + " | ");
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
