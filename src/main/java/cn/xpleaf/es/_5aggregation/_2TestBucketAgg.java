package cn.xpleaf.es._5aggregation;

import cn.xpleaf.es._1client.TestClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.children.Children;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.missing.MissingAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.ip.IpRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

/**
 * 桶聚合
 */
public class _2TestBucketAgg {
    TransportClient client;

    @Before
    public void init() throws Exception {
        client = TestClient.getClient();
    }

    // Terms Aggregation 分组聚合
    @Test
    public void test01() throws Exception {
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders
                .terms("per_count")
                .field("language");
        SearchResponse response = client
                .prepareSearch("books")
                .addAggregation(termsAggregationBuilder)
                .execute().actionGet();
        Terms terms = response.getAggregations().get("per_count");
        for(Terms.Bucket term : terms.getBuckets()) {
            System.out.println(term.getKey() + "---" + term.getDocCount());
        }
    }

    // Filter Aggregation 过滤器聚合
    // 把符合过滤器中的条件的文档分到一个桶中
    @Test
    public void test02() throws Exception {
        TermQueryBuilder filter = QueryBuilders.termQuery("title", "java");
        FilterAggregationBuilder filterAggregationBuilder = AggregationBuilders
                .filter("agg", filter);
        SearchResponse response = client
                .prepareSearch("books")
                .addAggregation(filterAggregationBuilder)
                .execute().actionGet();
        Filter agg = response.getAggregations().get("agg");
        System.out.println(agg.getDocCount());
    }

    // Filters Aggregation 多过滤器聚合
    // 把符合多个过滤条件的文档分到不同的桶中
    @Test
    public void test03() throws Exception {
        TermQueryBuilder filter1 = QueryBuilders.termQuery("title", "java");
        TermQueryBuilder filter2 = QueryBuilders.termQuery("title", "python");
        FiltersAggregationBuilder filtersAggregationBuilder = AggregationBuilders
                .filters("agg",
                        new FiltersAggregator.KeyedFilter("java", filter1),
                        new FiltersAggregator.KeyedFilter("python", filter2));
        SearchResponse response = client
                .prepareSearch("books")
                .addAggregation(filtersAggregationBuilder)
                .execute().actionGet();
        Filters agg = response.getAggregations().get("agg");
        for(Filters.Bucket bucket : agg.getBuckets()) {
            String key = bucket.getKeyAsString();
            long docCount = bucket.getDocCount();
            System.out.println(key + "---" + docCount);
        }
    }

    // Range Aggregation 范围聚合，用于反映数据的分布情况
    @Test
    public void test04() throws Exception {
        RangeAggregationBuilder rangeAggregationBuilder = AggregationBuilders
                .range("agg")
                .field("price")
                .addUnboundedTo(50)
                .addRange(50, 80)
                .addUnboundedFrom(80);
        SearchResponse response = client
                .prepareSearch("books")
                .addAggregation(rangeAggregationBuilder)
                .execute().actionGet();
        Range agg = response.getAggregations().get("agg");
        for(Range.Bucket bucket : agg.getBuckets()) {
            String key = bucket.getKeyAsString();
            Number from = (Number)bucket.getFrom();
            Number to = (Number)bucket.getTo();
            long docCount = bucket.getDocCount();
            System.out.println(key + "--" + docCount);
        }
    }

    // Date Range Aggregation 日期类型的范围聚合
    @Test
    public void test05() throws Exception {
        DateRangeAggregationBuilder dateRangeAggregationBuilder = AggregationBuilders
                .dateRange("agg")
                .field("publish_time")
                .format("yyyy-MM-dd")
                .addUnboundedTo("now-24M/M")
                .addUnboundedFrom("now-24M/M");
        SearchResponse response = client
                .prepareSearch("books")
                .addAggregation(dateRangeAggregationBuilder)
                .execute().actionGet();
        Range agg = response.getAggregations().get("agg");
        for(Range.Bucket bucket : agg.getBuckets()) {
            String key = bucket.getKeyAsString();
            DateTime fromAsDate = (DateTime)bucket.getFrom();
            DateTime toAsDate = (DateTime)bucket.getTo();
            long docCount = bucket.getDocCount();
            System.out.println(key + "--" + docCount);
        }
    }

    // Missing Aggregation 空值聚合，把文档集中所有缺失字段的文档分到一个桶中
    @Test
    public void test06() throws Exception {
        MissingAggregationBuilder missingAggregationBuilder = AggregationBuilders
                .missing("agg")
                .field("price");
        SearchResponse response = client
                .prepareSearch("books")
                .addAggregation(missingAggregationBuilder)
                .execute().actionGet();
        Missing agg = response.getAggregations().get("agg");
        System.out.println(agg.getDocCount());
    }

    // Children Aggregation 特殊的单桶聚合，可以根据父子文档关系进行分桶
    @Test
    public void test07() throws Exception {
        // 统计子类型为employee的文档数量
        AggregationBuilder childrenAggregationBuilder = AggregationBuilders
                .children("agg", "employee");
        SearchResponse response = client
                .prepareSearch("company")
                .addAggregation(childrenAggregationBuilder)
                .execute().actionGet();
        Children agg = response.getAggregations().get("agg");
        System.out.println(agg.getDocCount());
    }

    // IP Range Aggregation IP类型数据范围聚合
    @Test
    public void test08() throws Exception {
        // 统计*-100.0.0.5和100.0.0.5-*的IP地址数量
        IpRangeAggregationBuilder ipRangeAggregationBuilder = AggregationBuilders
                .ipRange("agg")
                .field("ip")
                .addUnboundedTo("10.0.0.5")
                .addUnboundedFrom("10.0.0.5");
        SearchResponse response = client
                .prepareSearch("ip_test")
                .addAggregation(ipRangeAggregationBuilder)
                .execute().actionGet();
        Range agg = response.getAggregations().get("agg");
        for(Range.Bucket bucket : agg.getBuckets()) {
            String key = bucket.getKeyAsString();
//            String fromAsString = bucket.getFromAsString();
//            String toAsString = bucket.getToAsString();
            long docCount = bucket.getDocCount();
            System.out.println(key + "--" + docCount);
        }
    }
}
