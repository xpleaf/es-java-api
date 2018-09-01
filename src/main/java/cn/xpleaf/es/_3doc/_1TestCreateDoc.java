package cn.xpleaf.es._3doc;

import cn.xpleaf.es._1client.TestClient;
import cn.xpleaf.es._3doc.domain.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 测试新建文档
 * 使用数据：
 * {
 *   "user":"kimchy",
 *   "postDate":"2013-01-30",
 *   "message":"trying out Elasticsearch"
 * }
 */
public class _1TestCreateDoc {
    TransportClient client;

    @Before
    public void init() throws Exception {
        client = TestClient.getClient();
    }

    // 方法一：将JSON转为String
    @Test
    public void test01() throws Exception {
        String doc = "{\n" +
                "  \"user\":\"kimchy\",\n" +
                "  \"postDate\":\"2013-01-30\",\n" +
                "  \"message\":\"trying out Elasticsearch\"\n" +
                "}";
        IndexResponse res = client.prepareIndex("twitter", "tweet", "1").setSource(doc).get();
        System.out.println(res.status());
    }

    // 方法二：使用Map
    @Test
    public void test02() throws Exception {
        Map<String, Object> doc = new HashMap<String, Object>(){
            {
                put("user", "kimchy");
                put("postDate", "2013-01-30");
                put("message", "trying out Elasticsearch");
            }
        };
        IndexResponse res = client.prepareIndex("twitter", "tweet", "2").setSource(doc).get();
        System.out.println(res.status());
    }

    // 方法三：使用Elasticsearch帮助类
    @Test
    public void test03() throws Exception {
        XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", "2013-01-30")
                .field("message", "trying out Elasticsearch")
                .endObject();
        System.out.println(doc.string());
        IndexResponse res = client.prepareIndex("twitter", "tweet", "3").setSource(doc).get();
        System.out.println(res.status());

        /**
         * 使用另外一种方法构建doc：jsonBuilder的startArray()方法（更复杂json文档的构建方式）
         * {
         *   "name":"Tom",
         *   "age":"12",
         *   "scores":[{"Math":"80","English":"85"}],
         *   "address":{
         *     "country":"China",
         *     "city":"Beijing"
         *   }
         * }
         */
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .field("name", "Tom")
                    .field("age", "12")
                    .startArray("scores")
                        .startObject().field("Math", "80").endObject()
                        .startObject().field("English", "85").endObject()
                    .endArray()
                    .startObject("address")
                        .field("country", "China")
                        .field("city", "Beijing")
                    .endObject()
                .endObject();
        System.out.println(builder.string());
    }

    // 方法四：使用Jackson序列化Java Bean
    @Test
    public void test04() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date date = sdf.parse("2013-01-30");
        User user = new User("Zhang san", date, "trying out Elasticsearch");
        ObjectMapper mapper = new ObjectMapper();
        mapper.setDateFormat(sdf);
        byte[] doc = mapper.writeValueAsBytes(user);
        IndexResponse res = client.prepareIndex("twitter", "tweet", "5").setSource(doc).execute().actionGet();
        System.out.println(res.status());
    }

    // 通过IndexResponse对象获取反馈信息
    @Test
    public void test05() throws Exception {
        Map<String, Object> doc = new HashMap<String, Object>(){
            {
                put("user", "kimchy");
                put("postDate", "2013-01-30");
                put("message", "trying out Elasticsearch");
            }
        };
        IndexResponse res = client.prepareIndex("twitter", "tweet", "2").setSource(doc).get();

        // 获取请求的索引名称
        String index = res.getIndex();
        // 获取请求的文档类型
        String type = res.getType();
        // 获取请求的文档ID
        String id = res.getId();
        // 获取文档版本
        long version = res.getVersion();
        // 返回文档是否创建成功
        // 如果文档是新创建的，就返回CREATED;如果文档不是首次创建而是被更新过的，就返回OK
        RestStatus status = res.status();

        System.out.println(String.format("index: %s, type: %s, id: %s, version: %s, status: %s",
                                        index, type, id, version, status));
    }






















}
