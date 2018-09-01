package cn.xpleaf.es._4search;

import cn.xpleaf.es._1client.TestClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.net.UnknownHostException;

public class EsMatchQueryTest {

    public static void main(String[] args) throws UnknownHostException {
        // 创建matchQueryBuilder对象
        MatchQueryBuilder matchQueryBuilder = QueryBuilders
                .matchQuery("title", "python")
                .operator(Operator.AND);

        // 创建highlightBuilder对象
        HighlightBuilder highlightBuilder = new HighlightBuilder()
                .field("title")
                .preTags("<span style=\"color:red\"")
                .preTags("</span>");

        TransportClient client = TestClient.getClient();
        SearchResponse response = client
                .prepareSearch("books")
                .setQuery(matchQueryBuilder)
                .highlighter(highlightBuilder)
                .setSize(100)   // 一次查询返回文档的数量
                .get();
        SearchHits hits = response.getHits();
        System.out.println(String.format("共搜索到：%s 条数据", hits.getTotalHits()));
        // 遍历搜索结果
        for(SearchHit hit : hits) {
            System.out.println("-----------------------------------");
            // 输出相关属性
            System.out.println("Source: " + hit.getSourceAsString());
            System.out.println("Source As Map: " + hit.getSource());
            System.out.println("Index: " + hit.getIndex());
            System.out.println("Type: " + hit.getType());
            System.out.println("ID: " + hit.getType());
            System.out.println("Price: " + hit.getSource().get("price"));
            System.out.println("Score: " + hit.getScore());
            // 获取高亮字段内容
            Text[] texts = hit.getHighlightFields().get("title").getFragments();
            if(texts != null) {
                for(Text text : texts) {
                    System.out.println(text.string());
                }
            }
        }
    }

}
