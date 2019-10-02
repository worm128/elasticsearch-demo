package com.zyj.fill;


import com.zyj.model.TweetMod;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * 批量请求
 */
public class Elasticsearch1BulkDemo {

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.233.133", 9200, "http"),
                        new HttpHost("192.168.233.133", 9201, "http"),
                        new HttpHost("192.168.233.133", 9202, "http")
                ));

        BulkRequest request = new BulkRequest();
        //新增数据
        IndexRequest index = new IndexRequest("twitter3", "tweet");
        //记录1
        index.source(XContentType.JSON, "message", "111", "name", "aa1", "phone", 15918837225L);
        request.add(index);
        //记录2
        index.source(XContentType.JSON, "message", "222", "name", "aa2", "phone", 15918837225L);
        request.add(index);

        //删除数据
//        DeleteRequest del = new DeleteRequest("twitter3", "tweet", "3");
//        request.add(del);
//        //更新数据
//        request.add(new UpdateRequest("posts", "doc", "2").doc(XContentType.JSON, "other", "test"));


        request.timeout(TimeValue.timeValueMinutes(2));

        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT); //异步同 添加一样

        for (BulkItemResponse bulkItemResponse : response) {
            //            bulkItemResponse.isFailed()
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();
            if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                    || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                IndexResponse indexResponse = (IndexResponse) itemResponse;
                System.out.println("新增:" + indexResponse);
            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                System.out.println("更新:" + updateResponse);
            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                System.out.println("删除:" + deleteResponse);
            }
        }

        //如果存在错误
        if (response.hasFailures()) {
            for (BulkItemResponse bulkItemResponse : response) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    System.out.println(failure);
                }
            }
        }

        client.close();
    }
}
