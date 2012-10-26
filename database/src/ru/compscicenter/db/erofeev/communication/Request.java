package ru.compscicenter.db.erofeev.communication;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.*;
import java.util.List;
import java.util.Map;

public class Request {

    public RequestType type;
    public Serializable data;
    Map<String, List<String>> params;

    public Request(RequestType type, Serializable data, Map<String, List<String>> params) {
        this.type = type;
        this.data = data;
        this.params = params;
    }

    @Override
    public String toString() {
        return type.name() + " " + " " + data + " " + params;
    }

    public Response send(String server, int port) throws IOException {
        System.out.println("start send");
        HttpClient client = new DefaultHttpClient();
        org.apache.http.client.methods.HttpUriRequest request = null;
        String address = "http://" + server + ":" + port;
        if (type == RequestType.put) {
            HttpPut hp = new HttpPut(address);
            ByteArrayEntity bae = new ByteArrayEntity(SerializationStuff.getBytes(data));
            hp.setEntity(bae);
            request = hp;
        } else if (type == RequestType.get) {
            request = new HttpGet(address);

        } else {
            request = new HttpDelete(address);
        }
        request.setHeader("Id", ids + "");
        HttpResponse response = null;
        try {
            response = client.execute(request);
            return new Response(response, this);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
