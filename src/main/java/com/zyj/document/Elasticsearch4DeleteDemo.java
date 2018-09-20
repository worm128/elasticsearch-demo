package com.zyj.document;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Map;

/**
 * 删除索引
 */
public class Elasticsearch4DeleteDemo {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.142.134", 9200, "http"),
                        new HttpHost("192.168.142.133", 9200, "http")));
        try {
            DeleteRequest  request = new DeleteRequest("posts","doc","1");

           // request.routing("routing");
          // request.parent("parent");

            //设置超时时间 默认 1m
             request.timeout(TimeValue.timeValueMinutes(2));

             //设置版本号 内部版本 若不一致会报 版本冲突异常
           //   request.version(23);
           //设置版本类型   可使用外部版本，  大于当前版本即可删除
//           request.versionType(VersionType.EXTERNAL);

            //刷新策略
            // NONE  不刷新  默认
            // IMMEDIATE  强制刷新作为此请求的一部分。此刷新策略不会针对高索引或搜索吞吐量进行扩展，但对于为流量非常低的索引提供一致的视图非常有用。这对测试来说非常棒！
            // WAIT_UNTIL  保持此请求处于打开状态，直到刷新使此请求的内容对搜索可见。此刷新策略与高索引和搜索吞吐量*兼容，但它会导致请求等待回复，直到刷新发生
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL); //刷新策略

            //异步通新增接口
            DeleteResponse response = client.delete(request);

            // client.deleteAsync();
            System.out.println(response);
            if (response.getResult() ==  DocWriteResponse.Result.NOT_FOUND) {
                System.out.println("文档不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ElasticsearchException e) {
            System.out.println(e);
            if (e.status() == RestStatus.NOT_FOUND) {
                System.out.println("未找到文档");
            } else  if (e.status() == RestStatus.CONFLICT) {
                System.out.println("版本冲突");
            }
        }finally {
            client.close();
        }
    }
}
