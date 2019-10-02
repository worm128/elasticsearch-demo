package com.zyj.fill;


import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPObject;
import com.alibaba.fastjson.TypeReference;
import com.zyj.model.TweetMod;
import org.apache.commons.codec.binary.StringUtils;
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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
        TweetMod tweetMod = new TweetMod();
        tweetMod.setPhone(15918837225L);
        tweetMod.setName("张三");
        tweetMod.setMessage("测试数据234324");
        IndexRequest indexRequest = addData("twitter3", "tweet", tweetMod);
        //request.add(indexRequest);

        //更新数据
        tweetMod = new TweetMod();
        tweetMod.setId("QD7ni20BwHV6EyqUcmBu");
        tweetMod.setPhone(15918837225L);
        tweetMod.setName("张三");
        tweetMod.setMessage("修改1");
        UpdateRequest updateRequest = updateData("twitter3", "tweet", tweetMod);
        //request.add(updateRequest);

        //删除数据
        tweetMod = new TweetMod();
        tweetMod.setName("张");
        List<TweetMod> list = search(client, "twitter3", "tweet", tweetMod);
        for (TweetMod t : list) {
            DeleteRequest del = new DeleteRequest("twitter3", "tweet", t.getId());
            request.add(del);
        }

        request.timeout(TimeValue.timeValueMinutes(2));
        request.waitForActiveShards(0);

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

    private static IndexRequest addData(String indexName, String typeName, TweetMod tweetMod) {
        //=============== 方式一 ====================
        //插入数据对象
        IndexRequest index = new IndexRequest(indexName, typeName);
        //转换json
        String json = JSONObject.toJSONString(tweetMod);
        //设置数据源
        index.source(json, XContentType.JSON);

        //=============== 方式二 ====================
        index.source(XContentType.JSON, "message", tweetMod.getMessage(), "name", tweetMod.getName(),
                "phone", tweetMod.getPhone());

        return index;
    }

    private static UpdateRequest updateData(String indexName, String typeName, TweetMod tweetMod) {
        //=============== 方式一 ====================
        //插入数据对象
        UpdateRequest index = new UpdateRequest(indexName, typeName, tweetMod.getId());
        //转换json
        String json = JSONObject.toJSONString(tweetMod);
        //设置数据源
        index.doc(json, XContentType.JSON);

        return index;
    }


    private static List<TweetMod> search(RestHighLevelClient client, String indexName, String typeName, TweetMod tweetMod) {
        try {
            List<TweetMod> list = new ArrayList<>();
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.timeout(new TimeValue(2, TimeUnit.SECONDS));
            //布尔类型查询（合并条件查询）
            BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();

            if (StrUtil.isNotBlank(tweetMod.getName())) {
                TermQueryBuilder termQueryBuilder1 = QueryBuilders.termQuery("name", tweetMod.getName());
                boolBuilder.must(termQueryBuilder1);
            }
            if (StrUtil.isNotBlank(tweetMod.getMessage())) {
                TermQueryBuilder termQueryBuilder2 = QueryBuilders.termQuery("message", tweetMod.getMessage());
                boolBuilder.must(termQueryBuilder2);
            }
            if (tweetMod.getPhone() != null) {
                TermQueryBuilder termQueryBuilder3 = QueryBuilders.termQuery("phone", tweetMod.getPhone());
                boolBuilder.must(termQueryBuilder3);
            }

            //查询建立
            sourceBuilder.query(boolBuilder);

            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.types(typeName);
            searchRequest.source(sourceBuilder);
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            for (SearchHit searchHit : hits) {
                String json = searchHit.getSourceAsString();
                TweetMod t = JSONObject.parseObject(json, TweetMod.class);
                t.setId(searchHit.getId());
                list.add(t);
            }

            return list;
        } catch (Exception e) {
            System.out.println(e);
        }
        return new ArrayList<>();
    }
}
