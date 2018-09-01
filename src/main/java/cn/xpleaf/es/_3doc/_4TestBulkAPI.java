package cn.xpleaf.es._3doc;

import cn.xpleaf.es._1client.TestClient;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 批量操作
 */
public class _4TestBulkAPI {
    TransportClient client;

    @Before
    public void init() throws Exception {
        client = TestClient.getClient();
    }

    // 使用Bulk API通过一次请求完成批量索引文档、批量删除文档和批量更新文档
    @Test
    public void test01() throws Exception {
        // 创建BulkRequestBuilder对象用于执行批量操作
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        // 使用IndexRequestBuilder创建索引文档请求对象
        XContentBuilder builder1 = XContentFactory.jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "another message")
                .endObject();
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("twitter", "tweet", "5").setSource(builder1);

        // 使用DeleteRequestBuilder创建删除文档请求对象
        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete("twitter", "tweet", "2");

        // 使用UpdateRequestBuilder创建索引文档请求对象
        XContentBuilder builder2 = XContentFactory.jsonBuilder()
                .startObject()
                .field("message", "update request")
                .endObject();
        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate("twitter", "tweet", "5").setDoc(builder2);

        // 添加对象到BulkRequestBuilder中
        BulkResponse bulkItemResponses = bulkRequestBuilder
                .add(indexRequestBuilder)
                .add(deleteRequestBuilder)
                .add(updateRequestBuilder)
                .execute().actionGet();

        // 遍历bulkItemResponses
        for(BulkItemResponse bulkItemResponse : bulkItemResponses) {
            DocWriteResponse response = bulkItemResponse.getResponse();
            if (response != null) {
                System.out.println(response.status());
            }
        }
    }

    // 使用Bulk Processor API在批量操作完成之前和之后进行相应的操作
    @Test
    public void test02() throws Exception {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            // 批量操作前
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("---批量操作前---");
                int numberOfActions = request.numberOfActions();
                System.out.println(String.format("numberOfActions: %s", numberOfActions));
                System.out.println("---批量操作前---");
            }

            // 批量操作后
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("---批量操作后---");
                int status = response.status().getStatus();
                System.out.println(String.format("status: %s", status));
                System.out.println("---批量操作后---");
            }

            // 批量操作出现异常时
            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("---批量操作出现异常了---");
                failure.printStackTrace();
                System.out.println("---批量操作出现异常了---");
            }
        };

        // 创建BulkProcessor对象，并设置相关属性
        BulkProcessor bulkProcessor = BulkProcessor
                .builder(client, listener)
                .setBulkActions(2)                                          // 设置请求操作的数据超过2次触发批量提交操作
                .setBulkSize(new ByteSizeValue(20, ByteSizeUnit.MB))   // 设置批处理请求达到20M触发批量提交动作
                .setFlushInterval(TimeValue.timeValueSeconds(5))            // 设置刷新索引时间间隔
                .setConcurrentRequests(5)                                   // 设置并发处理线程个数
                .setBackoffPolicy(BackoffPolicy
                        .exponentialBackoff(TimeValue.timeValueMillis(100),
                                3))                       // 设置回滚策略，等待时间为100ms，retry次数为3次
                .build();

        // 之后便可以通过add方法添加请求，达到设置的相关值时，就会触发批量请求操作
        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete("twitter", "tweet", "2");
        XContentBuilder builder1 = XContentFactory.jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "another message")
                .endObject();
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("twitter", "tweet", "5").setSource(builder1);
        bulkProcessor.add(deleteRequestBuilder.request());
        bulkProcessor.add(indexRequestBuilder.request());
    }
}
