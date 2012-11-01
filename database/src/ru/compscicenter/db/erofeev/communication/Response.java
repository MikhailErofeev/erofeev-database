package ru.compscicenter.db.erofeev.communication;

import org.apache.http.HttpResponse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Response {
    private Serializable data;
    private Code code;
    private Map<String, List<String>> params;

    public enum Code {
        OK(200), NOT_FOUND(404), BAD_REQUEST(400),
        FORBIDDEN(403), METHOD_NOT_ALLOWED(405),
        LENGTH_REQUIRED(411), UNDEFINED(-1);

        final int code;

        Code(int i) {
            this.code = i;
        }

        static Code getCode(int code) {
            for (Code c : Code.values()) {
                if (c.code == code) {
                    return c;
                }
            }
            return UNDEFINED;
        }
    }

    public Response(Code code, Serializable data) {
        this.code = code;
        this.data = data;
        this.params = new HashMap<String, List<String>>();
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

    Map<String, List<String>> getParams() {
        return params;
    }

    public static int getResponceCode(HttpResponse hp) {
        return hp.getStatusLine().getStatusCode();
    }

    public Serializable getData() {
        return data;
    }

    public void setData(Serializable data) {
        this.data = data;
    }

    public Code getCode() {
        return code;
    }

    public void setCode(Code code) {
        this.code = code;
    }


    @Override
    public String toString() {
        return "Response code: " + code + ". Data: " + (data != null ? data : "null");
    }

}
