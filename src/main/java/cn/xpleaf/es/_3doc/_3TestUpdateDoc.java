package cn.xpleaf.es._3doc;

import cn.xpleaf.es._1client.TestClient;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.junit.Before;
import org.junit.Test;

/**
 * 更新文档
 */
public class _3TestUpdateDoc {
    TransportClient client;

    @Before
    public void init() throws Exception {
        client = TestClient.getClient();
    }

    // 方法一：UpdateRequest jsonBuilder
    @Test
    public void test01() throws Exception {
        UpdateRequest request = new UpdateRequest();
        request.index("twitter");
        request.type("tweet");
        request.id("1");
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("gender", "male")
                .endObject();
        request.doc(builder);
        UpdateResponse res = client.update(request).get();
        System.out.println(res.status());
    }

    // 方法二：UpdateRequest内嵌脚本（map也是可以的）
    @Test
    public void test02() throws Exception {
        UpdateRequest request = new UpdateRequest("twitter", "tweet", "2")
                .script(new Script("ctx._source.gender = \"male\""));
        UpdateResponse res = client.update(request).get();
        System.out.println(res.status());
    }

    // 方法三：prepareUpdate
    @Test
    public void test03() throws Exception {
        // 内嵌脚本方式
        UpdateResponse res1 = client.prepareUpdate("twitter", "tweet", "1")
                .setScript(new Script("ctx._source.gender = \"female\"")).get();
        System.out.println(res1.status());

        // jsonBuilder方式
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("gender", "female")
                .endObject();
        UpdateResponse res2 = client.prepareUpdate("twitter", "tweet", "2").setDoc(builder).get();
        System.out.println(res2.status());
    }

    // 方法四：UpdateRequest内嵌脚本 upsert操作
    @Test
    public void test04() throws Exception {
        // 构建indexRequest
        XContentBuilder builder1 = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "Joe Smith")
                .field("gender", "male")
                .endObject();
        IndexRequest indexRequest = new IndexRequest("twitter", "tweet", "1").source(builder1);
        // 构建updateRequest对象
        XContentBuilder builder2 = XContentFactory.jsonBuilder()
                .startObject()
                .field("gender", "male")
                .endObject();
        UpdateRequest updateRequest = new UpdateRequest("twitter", "tweet", "1").doc(builder1)
                .upsert(indexRequest);

        // 如果文档twitter/tweet/1存在，就执行updateRequest操作，把gender修改为male
        // 如果文档twitter/tweet/1不存在，就执行indexRequest操作，新建一个文档
        UpdateResponse res = client.update(updateRequest).actionGet();
        System.out.println(res.status());
    }

}
