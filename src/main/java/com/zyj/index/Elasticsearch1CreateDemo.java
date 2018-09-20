package com.zyj.index;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
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

public class Elasticsearch1CreateDemo {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.142.134", 9200, "http"),
                        new HttpHost("192.168.142.133", 9200, "http")));


        CreateIndexRequest request = new CreateIndexRequest("twitter5");
        //配置
        request.settings(Settings.builder().
                put("index.number_of_shards",3).
                put("index.number_of_replicas",2));

        //设置超时
        request.timeout(TimeValue.timeValueMinutes(2));

        //超时连接到主节点的时间
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        //在创建索引API返回响应之前要等待的活动分片副本数
        request.waitForActiveShards(2);

        //同文档创建 可 json, 可 map, 可 XContentBuilder
        mappingByJsonStr(request);

        CreateIndexResponse createIndexResponse = client.indices().create(request);
        System.out.println(createIndexResponse.index());
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println("指示是否所有节点都已确认请求:" + acknowledged);
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
        System.out.println("指示在超时之前是否为索引中的每个分片启动了必需数量的分片副本:"+ shardsAcknowledged) ;

        client.close();
    }

    public static void mappingByJsonStr(CreateIndexRequest request) {
        request.mapping("tweet",
                "{\n" +
                        "  \"tweet\": {\n" +
                        "    \"properties\": {\n" +
                        "      \"message\": {\n" +
                        "        \"type\": \"text\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                XContentType.JSON);
    }


    public static void mappingByMap(CreateIndexRequest request) {
        Map<String, Object> jsonMap = new HashMap<>();
        Map<String, Object> message = new HashMap<>();
        message.put("type", "text");
        Map<String, Object> properties = new HashMap<>();
        properties.put("message", message);
        Map<String, Object> tweet = new HashMap<>();
        tweet.put("properties", properties);
        jsonMap.put("tweet", tweet);
        request.mapping("tweet", jsonMap);
    }

    public static void mappingByBuilder (CreateIndexRequest request) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("tweet");
            {
                builder.startObject("properties");
                {
                    builder.startObject("message");
                    {
                        builder.field("type", "text");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        request.mapping("tweet", builder);
        //request.mapping("tweet", "message", "type=text");
    }
}
