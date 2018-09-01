package cn.xpleaf.es._3doc;

import cn.xpleaf.es._1client.TestClient;
import org.elasticsearch.action.bulk.byscroll.BulkByScrollResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * 测试获取文档与删除文档
 */
public class _2TestGet_DelDoc {
    TransportClient client;

    @Before
    public void init() throws Exception {
        client = TestClient.getClient();
    }

    // 获取文档
    @Test
    public void test01() throws Exception {
        GetResponse res = client.prepareGet("twitter", "tweet", "6").get();

        boolean isExists = res.isExists();
        if(isExists) {
            // 获取文档的索引名
            String index = res.getIndex();
            // 获取文档的类型
            String type = res.getType();
            // 获取文档的id
            String id = res.getId();
            // 获取文档的版本
            long version = res.getVersion();
            // 获取文档的内容（二进制数组）
            byte[] bytes = res.getSourceAsBytes();
            // 获取文档的内容（map）
            Map<String, Object> map = res.getSourceAsMap();
            // 获取文档的内容（文本字符串方式）
            String content = res.getSourceAsString();
            // 判断文档是否为空
            boolean isEmpty = res.isSourceEmpty();

            System.out.println(content);
        }
    }

    // 删除文档
    @Test
    public void test02() throws Exception {
        DeleteResponse res = client.prepareDelete("twitter", "tweet", "1").get();
        // 删除成功，返回OK；删除失败，返回NOT_FOUND
        RestStatus status = res.status();
        System.out.println(status);
        // res.getXXX()有类似于创建文档时的其它api
    }

    // 查询删除
    @Test
    public void test03() throws Exception {
        DeleteByQueryRequestBuilder builder = DeleteByQueryAction.INSTANCE.newRequestBuilder(client);
        BulkByScrollResponse res = builder
                .filter(QueryBuilders.matchQuery("name", "Joe Smith"))  // 删除的Query
                .source("twitter")          // 索引名称
                .get();
        long deleted = res.getDeleted();    // 被删除文档的数目
        System.out.println(deleted);
    }

    // 批量获取
    @Test
    public void test04() throws Exception {
        MultiGetRequest.Item item = new MultiGetRequest.Item("another_index", "another_type", "anoterh_id");
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("twitter", "tweet", "1")
                .add("twitter", "tweet", "2", "3", "4")
                .add(item)
                .get();
        for(MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if(response !=null && response.isExists()) {
                String content = response.getSourceAsString();
                System.out.println(content);
            }
        }
    }

}
