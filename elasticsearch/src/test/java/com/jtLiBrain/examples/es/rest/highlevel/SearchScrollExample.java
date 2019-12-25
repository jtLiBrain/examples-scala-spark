package com.jtLiBrain.examples.es.rest.highlevel;

import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class SearchScrollExample extends ESTools {
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
        QueryBuilder query = QueryBuilders.matchQuery("title", "Elasticsearch");

    }

    public void execQuery(QueryBuilder query) throws IOException {
        SearchSourceBuilder sBuilder = new SearchSourceBuilder();
        sBuilder.query(query);

        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));

        SearchRequest sRequest = new SearchRequest("posts");
        sRequest.scroll(scroll);
        sRequest.source(sBuilder);

        SearchResponse searchResponse = client.search(sRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();

        while (searchHits != null && searchHits.length > 0) {
            /*
             *
             * Process the returned search results
             *
             */

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        boolean succeeded = clearScrollResponse.isSucceeded();
    }

}
