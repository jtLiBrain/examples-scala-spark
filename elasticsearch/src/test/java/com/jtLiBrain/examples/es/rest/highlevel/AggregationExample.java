package com.jtLiBrain.examples.es.rest.highlevel;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * 1. https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-search.html
 * 2. https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-query-builders.html
 */
public class AggregationExample extends ESTools {
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
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("groupBy-DestCountry")
                .field("DestCountry");

        aggregation.subAggregation(
                AggregationBuilders.sum("sum-totalDistance")
                        .field("DistanceKilometers")
        );

        SearchResponse response = execAggregation(aggregation);
        parseResponse(response);
    }

    private void parseResponse(SearchResponse response) {
        Aggregations aggregations = response.getAggregations();
        Terms byDestCountryAggregation = (Terms) aggregations.get("groupBy-DestCountry");

        for(Terms.Bucket bucket : byDestCountryAggregation.getBuckets()) {
            Aggregations subAggregations = bucket.getAggregations();
            Sum totalDistance = subAggregations.get("sum-totalDistance");

            println("DestCountry: " + bucket.getKeyAsString() + " TotalDistance: " + totalDistance.getValue());
        }
    }

    private SearchResponse execAggregation(AggregationBuilder aggregation) throws IOException {
        String index = "kibana_sample_data_flights";

        SearchSourceBuilder sBuilder = new SearchSourceBuilder();
        sBuilder.aggregation(aggregation);
        sBuilder.size(0);

        SearchRequest sRequest = new SearchRequest();
        sRequest.source(sBuilder);
        sRequest.indices(index);

        return client.search(sRequest, RequestOptions.DEFAULT);
    }
}
