package cn.xpleaf.es.client;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TestIndicesAdminClient {

    IndicesAdminClient indicesAdminClient;
    TransportClient client;

    @Before
    public void init() throws Exception {
        client = TestClient.getClient();
        indicesAdminClient = client.admin().indices();
    }

    // 判断索引是否存在
    @Test
    public void test01() throws Exception {
        IndicesExistsResponse response = indicesAdminClient.prepareExists("books").get();
        System.out.println(response.isExists());
    }

    // 判断type是否存在
    @Test
    public void test02() throws Exception {
        TypesExistsResponse response = indicesAdminClient.prepareTypesExists("books").setTypes("IT").get();
        System.out.println(response.isExists());
    }

    // 创建一个索引
    @Test
    public void test03() throws Exception {
        CreateIndexResponse response = indicesAdminClient.prepareCreate("es-java-api").get();
        System.out.println(response.isAcknowledged());
    }

    // 创建索引并设置Settings
    @Test
    public void test04() throws Exception {
        CreateIndexResponse response = indicesAdminClient.prepareCreate("twitter")
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 2)).get();
        System.out.println(response.isAcknowledged());
    }

    // 更新索引的副本数
    @Test
    public void test05() throws Exception {
        UpdateSettingsResponse response = indicesAdminClient.prepareUpdateSettings("twitter")
                .setSettings(Settings.builder()
                        .put("index.number_of_replicas", 0)).get();
        System.out.println(response.isAcknowledged());
    }

    // 获取Settings
    @Test
    public void test06() throws Exception {
        GetSettingsResponse response = indicesAdminClient.prepareGetSettings("twitter", "es-java-api").get();
        for(ObjectObjectCursor<String, Settings> cursor : response.getIndexToSettings()) {
            String index = cursor.key;
            Settings settings = cursor.value;
            Integer shards = settings.getAsInt("index.number_of_shards", null);
            Integer replicas = settings.getAsInt("index.number_of_replicas", null);
            System.out.println(String.format("indexName: %s, shards: %s, replicas: %s", index, shards, replicas));
        }
    }

    // 设置mapping
    @Test
    public void test07() throws Exception {
        indicesAdminClient.preparePutMapping("twitter")
                .setType("tweet")
                .setSource("{\n" +
                        "  \"properties\":{\n" +
                        "    \"name\":{\"type\":\"keyword\"}\n" +
                        "  }\n" +
                        "}").get();
    }

    // 获取mapping
    @Test
    public void test08() throws Exception {
        GetMappingsResponse response = indicesAdminClient.prepareGetMappings("twitter").get();
        ImmutableOpenMap<String, MappingMetaData> mappings = response.getMappings().get("twitter");
        MappingMetaData metaData = mappings.get("tweet");
        System.out.println(metaData.getSourceAsMap());
    }

    // 删除索引
    @Test
    public void test09() throws Exception {
        DeleteIndexResponse response = indicesAdminClient.prepareDelete("twitter").get();
        System.out.println(response.isAcknowledged());
    }

    // 刷新
    @Test
    public void test10() throws Exception {
        indicesAdminClient.prepareRefresh().get();
        indicesAdminClient.prepareRefresh("es-java-api").get();
        indicesAdminClient.prepareRefresh("es-java-api", "books").get();
    }

    // 关闭索引
    @Test
    public void test11() throws Exception {
        CloseIndexResponse response = indicesAdminClient.prepareClose("books").get();
        System.out.println(response.isAcknowledged());
    }

    // 打开索引
    @Test
    public void test12() throws Exception {
        OpenIndexResponse response = indicesAdminClient.prepareOpen("books").get();
        System.out.println(response.isAcknowledged());
    }

    // 设置别名
    @Test
    public void test13() throws Exception {
        IndicesAliasesResponse response = indicesAdminClient.prepareAliases().addAlias("es-java-api", "e3-commons-es").get();
        System.out.println(response.isAcknowledged());
    }

    // 获取别名
    @Test
    public void test14() throws Exception {
        GetAliasesResponse response = indicesAdminClient.prepareGetAliases("es-java-api").get();
    }
}
