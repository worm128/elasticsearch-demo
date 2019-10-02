package com.zyj.index;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Elasticsearch2DeleteDemo {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.233.133", 9200, "http"),
                        new HttpHost("192.168.233.133", 9201, "http"),
                        new HttpHost("192.168.233.133", 9202, "http")
                ));


        DeleteIndexRequest request = new DeleteIndexRequest ("twitter3");


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
