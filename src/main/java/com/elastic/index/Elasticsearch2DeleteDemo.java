package com.elastic.index;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * 删除索引
 */
public class Elasticsearch2DeleteDemo {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.50.130", 9200, "http"),
                        new HttpHost("192.168.50.130", 9201, "http"),
                        new HttpHost("192.168.50.130", 9202, "http")
                ));


        DeleteIndexRequest request = new DeleteIndexRequest("twitter3");

        //设置超时
        request.timeout(TimeValue.timeValueMinutes(2));

        //超时连接到主节点的时间
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));

        //在创建索引API返回响应之前要等待的活动分片副本数
        DeleteIndexResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
        boolean acknowledged = response.isAcknowledged();
        System.out.println("指示是否所有节点都已确认请求:" + acknowledged);

        client.close();
    }

}
