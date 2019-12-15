package com.jtLiBrain.examples.es.rest.highlevel;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public abstract class ESTools {
    protected RestHighLevelClient client;

    public void init() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
    }

    public void close() throws IOException {
        client.close();
    }

    public void println(Object obj) {
        System.out.println(obj);
    }
}
