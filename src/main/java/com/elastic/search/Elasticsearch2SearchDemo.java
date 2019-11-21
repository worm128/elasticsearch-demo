package com.elastic.search;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 *  查询
 */
public class Elasticsearch2SearchDemo {

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.142.134", 9200, "http"),
                        new HttpHost("192.168.142.133", 9200, "http")));

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //返回编号为20的帐户：

      //   searchSourceBuilder.query(QueryBuilders.matchQuery("account_number","20"));


        // bool查询返回地址中包含“mill”和“lane”的所有帐户
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        boolQueryBuilder.must(QueryBuilders.matchQuery("address", "mill"));
//        boolQueryBuilder.must(QueryBuilders.matchQuery("address", "lane"));
//        searchSourceBuilder.query(boolQueryBuilder);

         // 返回所有40岁 状态不是 ID
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        boolQueryBuilder.must(QueryBuilders.matchQuery("age", 40));
//        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("state", "ID"));
//        searchSourceBuilder.query(boolQueryBuilder);

        //返回所有余额 20000-3000之间的
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("balance");
        rangeQueryBuilder.gt(20000);
        rangeQueryBuilder.lt(30000);
        boolQueryBuilder.filter(rangeQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);





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
            System.out.println(sourceAsString);
        }

        client.close();
    }
}
