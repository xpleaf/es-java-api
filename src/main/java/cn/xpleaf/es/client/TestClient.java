package cn.xpleaf.es.client;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class TestClient {

    public static String CLUSTER_NAME = "elasticsearch";    // 集群名称
    public static String HOST_IP = "localhost";             // 服务器IP
    public static int TCP_PORT = 9300;                      // 端口

    public static void main(String[] args) throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", CLUSTER_NAME).build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST_IP), TCP_PORT));
        GetResponse getResponse = client.prepareGet("books", "IT", "1").get();
        System.out.println(getResponse.getSourceAsString());
    }

    public static TransportClient getClient() throws UnknownHostException {
        Settings settings = Settings.builder().put("cluster.name", CLUSTER_NAME).build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST_IP), TCP_PORT));
        return client;
    }
}
