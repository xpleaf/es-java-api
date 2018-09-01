package cn.xpleaf.es._5aggregation;

import cn.xpleaf.es._1client.TestClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;

import java.net.UnknownHostException;

public class MaxAggregationTest {
    public static void main(String[] args) throws UnknownHostException {
        MaxAggregationBuilder aggregationBuilder = AggregationBuilders
                .max("max_price")
                .field("price");
        SearchResponse response = TestClient.getClient()
                .prepareSearch("books")
                .addAggregation(aggregationBuilder)
                .get();
        Max agg = response.getAggregations().get("max_price");
        double value = agg.getValue();
        System.out.println(value);
    }
}
