package com.elastic.document;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Date;
import java.util.Map;


/**
 * 更新索引
 */
public class Elasticsearch5UpdateDemo {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.142.134", 9200, "http"),
                        new HttpHost("192.168.142.133", 9200, "http")));


        try {
            //source 同添加一样 可多种方法

            //注意 修改时候  同属性会更新，  不存在的属性会 合并到文档中
            UpdateRequest request = new UpdateRequest("posts", "doc", "0")
                    .doc("updated", new Date(),
                            "reason", "daily update");

         //   request.routing("routing");
      //      request.parent("parent");

            //请求超时时间  默认  1m
            request.timeout(TimeValue.timeValueMinutes(2));

            //刷新策略
            // NONE  不刷新  默认
            // IMMEDIATE  强制刷新作为此请求的一部分。此刷新策略不会针对高索引或搜索吞吐量进行扩展，但对于为流量非常低的索引提供一致的视图非常有用。这对测试来说非常棒！
            // WAIT_UNTIL  保持此请求处于打开状态，直到刷新使此请求的内容对搜索可见。此刷新策略与高索引和搜索吞吐量*兼容，但它会导致请求等待回复，直到刷新发生
           // request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL); //刷新策略


            //如果出现冲突  设置重试次数 默认为0
              request.retryOnConflict(3);

            //是否返回元数据 可通过response.getGetResult()  默认关闭
           request.fetchSource(true);

            UpdateResponse response = client.update(request);
            System.out.println(response);
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                System.out.println("处理首次创建文档的情况");
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                System.out.println("处理文档更新的案例");
            } else if (response.getResult() == DocWriteResponse.Result.DELETED) {
                System.out.println("处理删除文档的情况");
            } else if (response.getResult() == DocWriteResponse.Result.NOOP) {
                System.out.println("处理文档未受更新影响的情况，即未对文档执行任何操作");
            }


            //需要开启才可获取到数据 request.fetchSource(true);
            GetResult result = response.getGetResult();
            if (result.isExists()) {
                String sourceAsString = result.sourceAsString();
                System.out.println("str:" + sourceAsString);
                Map<String, Object> sourceAsMap = result.sourceAsMap();
                System.out.println("map:"+ sourceAsMap);
                byte[] sourceAsBytes = result.source();
                System.out.println("byte[]:" + sourceAsBytes);
            } else {
                System.out.println("获取不到文档");
            }

            ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                System.out.println("处理成功分片数与总分片数不一样");
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    System.out.println(reason);
                }
            }

        } catch (ElasticsearchException e) {
            System.out.println(e);
            if (e.status() == RestStatus.CONFLICT) {
                System.out.println("版本冲突");
            } else if (e.status() == RestStatus.NOT_FOUND) {
                System.out.println("文档不存在");
            }
        } finally {
            client.close();
        }

    }
}
