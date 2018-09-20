package com.zyj.document;


import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 批量请求
 */
public class Elasticsearch7BulkProcessorDemo {

    public static void main(String[] args) throws IOException, InterruptedException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.142.134", 9200, "http"),
                        new HttpHost("192.168.142.133", 9200, "http")));
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                System.out.println("调用之前，数量为："+ numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {
                System.out.println("调用之后调用");
                for (BulkItemResponse bulkItemResponse : response) {
                    DocWriteResponse itemResponse = bulkItemResponse.getResponse();
                    System.out.println(itemResponse);
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("出现异常" + failure);
            }
        };

        BulkProcessor.Builder builder = BulkProcessor.builder(client::bulkAsync, listener);
        //根据当前添加的操作数设置何时刷新新的批量请求（默认为1000，使用-1禁用它）
        builder.setBulkActions(500);
        //根据当前添加的操作大小设置何时刷新新的批量请求（默认为5Mb，使用-1禁用它）
        builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));
        //设置允许执行的并发请求数（默认为1，使用0只允许执行单个请求）
        builder.setConcurrentRequests(0);
        //BulkRequest如果间隔超过，则 设置刷新间隔刷新任何挂起（默认为未设置）
        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
        //设置一个最初等待1秒的常量退避策略，最多重试3次。
        builder.setBackoffPolicy(BackoffPolicy
                .constantBackoff(TimeValue.timeValueSeconds(1L), 3));

        IndexRequest one = new IndexRequest("posts", "doc", "1").
                source(XContentType.JSON, "title",
                        "In which order are my Elasticsearch queries executed?");
        IndexRequest two = new IndexRequest("posts", "doc", "2")
                .source(XContentType.JSON, "title",
                        "Current status and upcoming changes in Elasticsearch");
        IndexRequest three = new IndexRequest("posts", "doc", "3")
                .source(XContentType.JSON, "title",
                        "The Future of Federated Search in Elasticsearch");

        BulkProcessor bulkProcessor = builder.build();
        bulkProcessor.add(one);
        bulkProcessor.add(two);
        bulkProcessor.add(three);

        //是否已经全部执行完成  返回 false表示 超时还未执行完成
        boolean terminated = bulkProcessor.awaitClose(30, TimeUnit.SECONDS);
        System.out.println(terminated);
        if (terminated) {
            bulkProcessor.close();
            client.close();
        }


    }
}
