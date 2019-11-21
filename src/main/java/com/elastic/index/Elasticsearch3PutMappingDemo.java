package com.elastic.index;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 索引导入数据
 */
public class Elasticsearch3PutMappingDemo {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.50.130", 9200, "http"),
                        new HttpHost("192.168.50.130", 9201, "http"),
                        new HttpHost("192.168.50.130", 9202, "http")
                ));

        //库名(index)：twitter3
        PutMappingRequest request = new PutMappingRequest("twitter3");
        //表名(type)：tweet
        request.type("tweet");

        //设置超时
        request.timeout(TimeValue.timeValueMinutes(2));

        //超时连接到主节点的时间
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));

        //同文档创建 可 json, 可 map, 可 XContentBuilder
        mappingByJsonStr(request);

        PutMappingResponse response = client.indices().putMapping(request, RequestOptions.DEFAULT);
        //client.indices().putMappingAsync(); 异步请求

        boolean acknowledged = response.isAcknowledged();
        System.out.println("指示是否所有节点都已确认请求:" + acknowledged);

        client.close();
    }


    public static void mappingByJsonStr(PutMappingRequest request) {
        request.source(
                "{\n" +
                        "  \"properties\": {\n" +
                        "    \"message\": {\n" +
                        "      \"type\": \"text\"\n" +
                        "    },\n" +
                        "    \"name\": {\n" +
                        "      \"type\": \"text\"\n" +
                        "    },\n" +
                        "    \"phone\": {\n" +
                        "      \"type\": \"long\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                XContentType.JSON);
    }


    public static void mappingByMap(PutMappingRequest request) {
        Map<String, Object> jsonMap = new HashMap<>();
        Map<String, Object> message = new HashMap<>();
        message.put("type", "text");
        Map<String, Object> properties = new HashMap<>();
        properties.put("message", message);
        jsonMap.put("properties", properties);
        request.source(jsonMap);
    }

    public static void mappingByBuilder(PutMappingRequest request) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
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
        request.source(builder);
        // request.source("message", "type=text");
    }
}
