package com.elastic.document;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Date;


/*
*    异步创建索引
*/
public class Elasticsearch2AddDemo {

    public static void main(String[] args) throws IOException, InterruptedException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.142.134", 9200, "http"),
                        new HttpHost("192.168.142.133", 9200, "http")));

        IndexRequest request = getRequestByBuilder();

         client.indexAsync(request,new ActionListener<IndexResponse>() {
             @Override
             public void onResponse(IndexResponse indexResponse) {
                 System.out.println(indexResponse);
             }

             @Override
             public void onFailure(Exception e) {
                 System.err.println(e);
                 if (e instanceof ElasticsearchException) {
                     ElasticsearchException esException = (ElasticsearchException)e;
                     System.out.println(esException.status());
                     if (esException.status() == RestStatus.CONFLICT) {
                         System.out.println("版本冲突异常");
                     }
                 }
             }
         });

         Thread.sleep(3000L);
        client.close();

    }




    private static IndexRequest getRequestByBuilder() throws IOException {
        IndexRequest request = new IndexRequest("posts","doc","2");
        request.source("user", "kimchy", "postDate", new Date(), "message", "trying out Elasticsearch");
        //如果opType 设置为 CREATE  索引内容相同 也会抛出冲突异常ElasticsearchStatusException
        // request.source("user", "kimchy", "postDate", new Date(), "message", "trying out Elasticsearch").opType(DocWriteRequest.OpType.CREATE);
        //如果存在版本冲突会抛出异常：ElasticsearchStatusException
        // request.source("user", "kimchy", "postDate", new Date(), "message", "trying out Elasticsearch").version(5);
        return request;
    }
}

