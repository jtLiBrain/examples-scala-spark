package com.jtLiBrain.examples.es.rest.highlevel;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * 1. https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-search.html
 * 2. https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-query-builders.html
 */
public class QueryExample extends ESTools {
    @Before
    public void init() {
        super.init();
    }

    @After
    public void close() throws IOException {
        super.close();
    }


    @Test
    public void testTermQuery() throws IOException {
        QueryBuilder query = QueryBuilders.termQuery("DestCityName", "Shanghai");

        SearchResponse response = execQuery(query);
        System.out.println(response.toString());
    }

    public SearchResponse execQuery(QueryBuilder query) throws IOException {
        String index = "kibana_sample_data_flights";

        SearchSourceBuilder sBuilder = new SearchSourceBuilder();
        sBuilder.query(query);

        SearchRequest sRequest = new SearchRequest();
        sRequest.source(sBuilder);
        sRequest.indices(index);

        return client.search(sRequest, RequestOptions.DEFAULT);
    }
}
