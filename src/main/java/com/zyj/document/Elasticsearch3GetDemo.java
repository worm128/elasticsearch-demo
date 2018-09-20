package com.zyj.document;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * 获取索引
 */
public class Elasticsearch3GetDemo {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.142.134", 9200, "http"),
                        new HttpHost("192.168.142.133", 9200, "http")));
        try {
            GetRequest request = new GetRequest("posts","doc","1");

            //不获取元数据，   默认是 获取
        //     request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);

            //配置特定字段的源包含
//            String[] includes = Strings.EMPTY_ARRAY;
//            String[] excludes = new String[]{"message"};
//            FetchSourceContext fetchSourceContext =
//                    new FetchSourceContext(true, includes, excludes);
//            request.fetchSourceContext(fetchSourceContext);




            //为特定字段配置源排除
//             request.storedFields("message");

            //request.routing("routing");
           // request.parent("parent");
           // request.preference("preference");
            //request.storedFields("message"); ??
            //设置实时
            // request.realtime(false);
             //获取之前先刷新，  默认为false
//             request.refresh(true);

            //设置版本 若设置版本 如果版本与当前不一致则会报 冲突异常
           //  request.version(24);
            //设置版本类型   两个没区别
          //  request.versionType(VersionType.EXTERNAL_GTE);
            GetResponse response = client.get(request);
            System.out.println(response);
            //client.getAsync();  异步获取
            // 是否存在查询
            Boolean resp =  client.exists(request);
            System.out.println(resp);

            if (response.isExists()) {
                String sourceAsString = response.getSourceAsString();  //将文档检索为 String
                System.out.println("string:" + sourceAsString);
                Map<String, Object> sourceAsMap = response.getSourceAsMap();  //将文档检索为 Map<String, Object>
                System.out.println("map:"+ sourceAsMap);
                byte[] sourceAsBytes = response.getSourceAsBytes(); //将文档检索为 byte[]
                System.out.println("byte[]:" + sourceAsBytes);
            } else {
                System.out.println("未找到");
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
        } finally {
            client.close();
        }
    }
}
