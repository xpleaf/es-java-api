package cn.xpleaf.es._6cluster;

import cn.xpleaf.es._1client.TestClient;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;

import java.net.UnknownHostException;

public class ClusterClientTest {

    public static void main(String[] args) throws UnknownHostException {
        TransportClient client = TestClient.getClient();
        ClusterHealthResponse healths = client
                .admin()
                .cluster()
                .prepareHealth()
                .get();
        String clusterName = healths.getClusterName();
        int numberOfDataNodes = healths.getNumberOfDataNodes();
        int numberOfNodes = healths.getNumberOfNodes();
        System.out.println("---集群信息---");
        System.out.println(String.format("clusterName: %s, numberOfDataNodes: %s, numberOfNodes: %s",
                clusterName, numberOfDataNodes, numberOfNodes));
        System.out.println("---集群中各索引信息---");
        for(ClusterIndexHealth health : healths.getIndices().values()) {
            String index = health.getIndex();
            int numberOfShards = health.getNumberOfShards();
            int numberOfReplicas = health.getNumberOfReplicas();
            ClusterHealthStatus status = health.getStatus();
            System.out.println(String.format("index: %s, numberOfShards: %s, numberOfReplicas: %s, ClusterHealthStatus: %s",
                    index, numberOfShards, numberOfReplicas, status.value()));
        }
    }

}

/**
 * 执行后的输出信息：
 * ---集群信息---
 * clusterName: elasticsearch, numberOfDataNodes: 1, numberOfNodes: 1
 * ---集群中各索引信息---
 * index: bank_news, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: website, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: test, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: my-index, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: range_index, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: index, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: blog, numberOfShards: 3, numberOfReplicas: 0, ClusterHealthStatus: 0
 * index: geoshape, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: es-java-api, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: geo, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: index_1, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: index_2, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: twitter, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: bank, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: books, numberOfShards: 3, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: .kibana, numberOfShards: 1, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: bank_news_news, numberOfShards: 1, numberOfReplicas: 0, ClusterHealthStatus: 0
 * index: ip_test, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: my_index, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 * index: company, numberOfShards: 5, numberOfReplicas: 1, ClusterHealthStatus: 1
 */
