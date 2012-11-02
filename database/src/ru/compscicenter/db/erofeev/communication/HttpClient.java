package ru.compscicenter.db.erofeev.communication;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 10/26/12
 * Time: 7:38 PM
 */
public class HttpClient {

    private static HttpUriRequest prepareRequest(String address, Request request) {
        HttpUriRequest httpRequest = null;
        if (request.getType() == Request.RequestType.PUT) {
            HttpPut hp = new HttpPut(address);
            ByteArrayEntity bae = new ByteArrayEntity(SerializationStuff.getBytes(request.getData()));
            hp.setEntity(bae);
            httpRequest = hp;
        } else if (request.getType() == Request.RequestType.GET) {
            httpRequest = new HttpGet(address);
        } else {
            httpRequest = new HttpDelete(address);
        }
        for (Map.Entry<String, List<String>> entry : request.getParams().entrySet()) {
            for (String value : entry.getValue()) {
                httpRequest.setHeader(entry.getKey(), value);
            }
        }
        return httpRequest;
    }

    public abstract static class AsyncHttpClient implements Runnable {
        private String addr;
        private Request request;

        protected AsyncHttpClient(String addr, Request request) {
            this.addr = addr;
            this.request = request;
        }

        public abstract void performResponce(Response response);

        @Override
        public void run() {
            performResponce(HttpClient.sendRequest(addr, request));
        }
    }

    private static Response generateResponse(HttpResponse httpResponse) throws IOException {
        int length = Integer.valueOf(httpResponse.getFirstHeader("Content-Length").getValue());
        Serializable data = null;
        if (length > 0) {
            byte[] bytes = new byte[length];
            httpResponse.getEntity().getContent().read(bytes);
            data = SerializationStuff.getObject(bytes);
        }
        Response.Code code = Response.Code.getCode(httpResponse.getStatusLine().getStatusCode());
        Response response = new Response(code, data);
        for (org.apache.http.Header h : httpResponse.getAllHeaders()) {
            response.addParam(h.getName(), h.getValue());
        }
        return response;
    }

    public static Response sendRequest(String address, Request request) {
        try {
            DefaultHttpClient client = new DefaultHttpClient();
            HttpUriRequest httpUriRequest = prepareRequest(address, request);
            System.out.println("request send to " + address);
            HttpResponse httpResponse = client.execute(httpUriRequest);
            System.out.println("ok");
            return generateResponse(httpResponse);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
