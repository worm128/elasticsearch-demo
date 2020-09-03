package com.elastic.search;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 *  聚合
 */
public class Elasticsearch4SearchDemo {

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.50.130", 9200, "http"),
                        new HttpHost("192.168.50.130", 9201, "http"),
                        new HttpHost("192.168.50.130", 9202, "http")
                ));


        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(AggregationBuilders.terms("top_10_age").field("age").size(10));
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse =  client.search(searchRequest, RequestOptions.DEFAULT);
        RestStatus status = searchResponse.status();

        System.out.println(status);

        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }

        Aggregations aggregations = searchResponse.getAggregations();
        for (Aggregation aggregation : aggregations.asList()) {
            System.out.println(aggregation);
            System.out.println(aggregation.getName());
            System.out.println(aggregation.getMetaData());
        }


        client.close();
    }
}
