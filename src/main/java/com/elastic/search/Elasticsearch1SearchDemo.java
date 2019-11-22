package com.elastic.search;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 *  查询所有
 */
public class Elasticsearch1SearchDemo {

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.50.130", 9200, "http"),
                        new HttpHost("192.168.50.130", 9201, "http"),
                        new HttpHost("192.168.50.130", 9202, "http")
                ));

        SearchRequest searchRequest = new SearchRequest();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(3);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse =  client.search(searchRequest);
        RestStatus status = searchResponse.status();
        TimeValue took = searchResponse.getTook();
        Boolean terminatedEarly = searchResponse.isTerminatedEarly();
        boolean timedOut = searchResponse.isTimedOut();

        System.out.println(status);
        System.out.println(took);
        System.out.println(terminatedEarly);
        System.out.println(timedOut);

        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
//            String index = hit.getIndex();
//            String type = hit.getType();
//            String id = hit.getId();
//            float score = hit.getScore();
//
//            System.out.println(index);
//            System.out.println(type);
//            System.out.println(id);
//            System.out.println(score);

            String sourceAsString = hit.getSourceAsString();
            // hit.getSourceAsMap()
            System.out.println(sourceAsString);
        }

        client.close();
    }
}
