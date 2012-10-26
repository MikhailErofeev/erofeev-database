package ru.compscicenter.db.erofeev.communication;

import org.apache.http.HttpResponse;

import java.io.IOException;
import java.io.Serializable;

public class Response {
    private Serializable data;
    private int code;
    private Request rq;

    public Response(HttpResponse resp, Request rq) {
        byte[] bytes = new byte[Integer.valueOf(resp.getFirstHeader("Content-Length").getValue())];
        code = resp.getStatusLine().getStatusCode();
        this.rq = rq;
        try {
            resp.getEntity().getContent().read(bytes);
            data = Request.getObject(bytes);
        } catch (IOException e) {
        }
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

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public Request getRq() {
        return rq;
    }

    public void setRq(Request rq) {
        this.rq = rq;
    }

    @Override
    public String toString() {
        return "Request " + rq.type + ". Code: " + code + ". Data: " + (data != null ? data : "null");
    }

}
