package cn.xpleaf.es._5aggregation;

import cn.xpleaf.es._1client.TestClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.junit.Before;
import org.junit.Test;

/**
 * 指标聚合
 */
public class _1TestIndexAgg {
    TransportClient client;

    @Before
    public void init() throws Exception {
        client = TestClient.getClient();
    }

    // Min Aggregation 求最小值
    @Test
    public void test01() throws Exception {
        MinAggregationBuilder minAggregationBuilder = AggregationBuilders
                .min("min_price")
                .field("price");
        SearchResponse response = client
                .prepareSearch("books")
                .addAggregation(minAggregationBuilder)
                .execute().actionGet();
        Min min = response.getAggregations().get("min_price");
        double minValue = min.getValue();
        System.out.println(minValue);
    }

    // Sum Aggregation 求和
    @Test
    public void test02() throws Exception {
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders
                .sum("sum_price")
                .field("price");
        SearchResponse response = client
                .prepareSearch("books")
                .addAggregation(sumAggregationBuilder)
                .execute().actionGet();
        Sum sum = response.getAggregations().get("sum_price");
        double sumValue = sum.getValue();
        System.out.println(sumValue);
    }

    // Avg Aggregation 求平均值
    @Test
    public void test03() throws Exception {
        AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders
                .avg("avg_price")
                .field("price");
        SearchResponse response = client
                .prepareSearch("books")
                .addAggregation(avgAggregationBuilder)
                .execute().actionGet();
        Avg avg = response.getAggregations().get("avg_price");
        double avgValue = avg.getValue();
        System.out.println(avgValue);
    }

    // Stats Aggregation 基本统计
    @Test
    public void test04() throws Exception {
        StatsAggregationBuilder statsAggregationBuilder = AggregationBuilders
                .stats("stats_price")
                .field("price");
        SearchResponse response = client
                .prepareSearch("books")
                .addAggregation(statsAggregationBuilder)
                .execute().actionGet();
        Stats stats = response.getAggregations().get("stats_price");
        double min = stats.getMin();
        double max = stats.getMax();
        double avg = stats.getAvg();
        double sum = stats.getSum();
        long count = stats.getCount();
        System.out.println(String
                .format("Min: %s, Max: %s, Avg: %s, Sum: %s, Count: %s", min, max, avg, sum, count));
    }

    // Extended Stats Aggregation
    @Test
    public void test05() throws Exception {
        ExtendedStatsAggregationBuilder extendedStatsAggregationBuilder = AggregationBuilders
                .extendedStats("extended_stats_price")
                .field("price");
        SearchResponse response = client
                .prepareSearch("books")
                .addAggregation(extendedStatsAggregationBuilder)
                .execute().actionGet();
        ExtendedStats stats = response.getAggregations().get("extended_stats_price");
        double min = stats.getMin();
        double max = stats.getMax();
        double avg = stats.getAvg();
        double sum = stats.getSum();
        long count = stats.getCount();
        double stdDeviation = stats.getStdDeviation();  // 标准差
        double sumOfSquares = stats.getSumOfSquares();  // 平方和
        double variance = stats.getVariance();          // 方差
        System.out.println(String
                .format("Min: %s, Max: %s, Avg: %s, Sum: %s, Count: %s", min, max, avg, sum, count));
        System.out.println(String
                .format("stdDeviation: %s, sumOfSquares: %s, variance: %s", stdDeviation, sumOfSquares, variance));
    }

    // Cardinality Aggregation 基数统计
    // 其作用是先执行类似SQL中的distinct操作，去掉集合中的重复项，然后统计排重后的集合长度
    @Test
    public void test06() throws Exception {
        CardinalityAggregationBuilder cardAgg = AggregationBuilders
                .cardinality("agg")
                .field("language");
        SearchResponse response = client
                .prepareSearch("books")
                .addAggregation(cardAgg)
                .execute().actionGet();
        Cardinality cardValue = response.getAggregations().get("agg");
        System.out.println(cardValue.getValue());
    }

    // Value Count Aggregation 按字段统计文档数量
    @Test
    public void test07() throws Exception {
        ValueCountAggregationBuilder valueCountAggregationBuilder = AggregationBuilders
                .count("agg")
                .field("author");
        SearchResponse response = client
                .prepareSearch("books")
                .addAggregation(valueCountAggregationBuilder)
                .execute().actionGet();
        ValueCount valueCount = response.getAggregations().get("agg");
        long value = valueCount.getValue();
        System.out.println(value);
    }

}
