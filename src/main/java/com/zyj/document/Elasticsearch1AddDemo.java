package com.zyj.document;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/*
 *  同步创建索引
 */
public class Elasticsearch1AddDemo {

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.142.134", 9200, "http"),
                        new HttpHost("192.168.142.133", 9200, "http")
                        ));

        IndexRequest request = getRequestByBuilder();
        //设置操作类型  默认为INDEX 如果存在则替换， 不存在则新增
       //  request.opType(DocWriteRequest.OpType.CREATE);

        //设置版本号
         request.version(13);
        //版本类型
        // INTERNAL  内部版本 默认  指定的version字段和当前的version号相同
        //EXTERNAL  外部版本 必须指定version并且需要大于当前的版本号
        //          版本号必须是大于零的整数， 且小于 9.2E+18 — 一个 Java 中 long 类型的正值
         //          外部版本还可以在create时候指定
         request.versionType(VersionType.EXTERNAL);


        //request.routing("routing")
        //request.parent("parent");
       // request.setPipeline("pipeline");

        //请求超时时间  默认  1m
         request.timeout(TimeValue.timeValueMinutes(1));

        //刷新策略
        // NONE  不刷新  默认
        // IMMEDIATE  强制刷新作为此请求的一部分。此刷新策略不会针对高索引或搜索吞吐量进行扩展，但对于为流量非常低的索引提供一致的视图非常有用。这对测试来说非常棒！
        // WAIT_UNTIL  保持此请求处于打开状态，直到刷新使此请求的内容对搜索可见。此刷新策略与高索引和搜索吞吐量*兼容，但它会导致请求等待回复，直到刷新发生
      //   request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL); //刷新策略

        try {
          IndexResponse indexResponse = client.index(request);
          System.out.println(indexResponse);
//        String index = indexResponse.getIndex();
//        String type = indexResponse.getType();
//        String id = indexResponse.getId();
//        long version = indexResponse.getVersion();

            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                System.out.println("新建索引");
            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                System.out.println("更新索引");
            }
            ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                System.out.println("处理成功分片数小于总分片数");
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    //存在失败
                    System.out.println(reason);
                }
            }
        } catch (ElasticsearchException e) {
            System.err.println(e);
            if (e.status() == RestStatus.CONFLICT) {
                System.out.println("版本冲突异常");
            }
        } finally {
            client.close();
        }


    }


    private static IndexRequest getRequestByJsonStr() {
        //id传入null, 则自动生成
        IndexRequest request = new IndexRequest("posts","doc","0");
        String jsonString = "{" +
                "\"user\":\"kimchy3\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);
        return request;
    }

    private static IndexRequest getRequestByMap() {
        IndexRequest request = new IndexRequest("posts","doc","2");
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy2222");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        request.source(jsonMap);
        return request;
    }

    private static IndexRequest getRequestByBuilder() throws IOException {
        IndexRequest request = new IndexRequest("posts","doc","1");
//        XContentBuilder builder = XContentFactory.jsonBuilder();
//        builder.startObject();
//        {
//            builder.field("user", "lisi");
//            builder.timeField("postDate", new Date());
//            builder.field("message", "trying out Elasticsearch");
//        }
//        builder.endObject();
//        request.source(builder);
        request.source("user", "kimchy", "postDate", new Date(), "message", "trying out Elasticsearch");
        return request;
    }
}
