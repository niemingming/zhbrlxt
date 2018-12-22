package com.nmm.study.search;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 查询服务
 */
@Service
public class SearchService implements InitializingBean{
    private RestHighLevelClient restHighLevelClient;
    @Value("${elasticsearch.address}")
    private String address;
    private RestClient restClient;
    private String index = "sougou-123";
    //查询总数
    public long searchTotal() throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.size(0);
        SearchRequest searchRequest = new SearchRequest(index);
        long total = restHighLevelClient.search(searchRequest).getHits().getTotalHits();
        return total;
    }
    //统计用户查询前20 的信息
    public String searchTop20Info() throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.size(0);
        sourceBuilder.aggregation(AggregationBuilders.terms("users").field("userId").size(20));

        SearchRequest request = new SearchRequest(index);
        request.source(sourceBuilder);
        Terms terms = restHighLevelClient.search(request).getAggregations().get("users");
        StringBuilder res = new StringBuilder();
        int i = 1;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            res.append(i++).append("\t").append(bucket.getKeyAsString()).append("\t").append(bucket.getDocCount()).append("\n");
        }
        return res.toString();
    }
    //查询三者数据
    public String searchBg360() throws IOException {
        List<String> res = new ArrayList<>();
        long baidu = searchUrl("baidu.com");
        long google = searchUrl("google.com");
        long sl0 = searchUrl("360.com");
        res.add("baidu.com\t" + baidu);
        if (google > baidu) {
            res.add(0,"google.com\t" + google);
        }else {
            res.add("google.com\t" + google);
        }
        if (sl0 > Math.max(baidu,google)){
            res.add(0,"360.com\t" + google);
        }else if (sl0 < Math.min(baidu,google)){
            res.add("360.com\t" + google);
        }else {
            res.add(1,"360.com\t" + google);
        }
        StringBuilder resstr = new StringBuilder();
        int i = 1;
        for (String re : res) {
            resstr.append(i++).append("\t").append(re).append("\n");
        }
        return resstr.toString();
    }

    private long searchUrl(String s) throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.size(0);
        BoolQueryBuilder builder = QueryBuilders.boolQuery();
        sourceBuilder.query(builder);
        //添加条件
        builder.must(QueryBuilders.wildcardQuery("searchWord","*"+s+"*"));
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(sourceBuilder);
        long total = restHighLevelClient.search(searchRequest).getHits().getTotalHits();
        return total;
    }
    //第四题统计全天数据
    public String searchAllDay() throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.size(0);
        //分组
        sourceBuilder.aggregation(AggregationBuilders.histogram("times").field("visitTime").interval(3600));
        SearchRequest request = new SearchRequest(index);
        request.source(sourceBuilder);
        Histogram histogram = restHighLevelClient.search(request).getAggregations().get("times");
        StringBuilder res = new StringBuilder();
        histogram.getBuckets().forEach(b ->{
            long time = (long) (Double.parseDouble(b.getKeyAsString())/3600);
            String key = time < 10 ? "0" + time: time+"";
            key += ":00:00\t" + b.getDocCount();
            res.append(key).append("\n");
        });
        return res.toString();
    }

    public String searchFive() throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.size(0);
        //分组
        sourceBuilder.aggregation(AggregationBuilders.terms("users").size(9000).field("userId").subAggregation(
                AggregationBuilders.cardinality("sessions").field("sessionId")
        ));
        SearchRequest request = new SearchRequest(index);
        request.source(sourceBuilder);

        Terms terms = restHighLevelClient.search(request).getAggregations().get("users");

        List<String> res = new ArrayList<>();
        List<Long> totals = new ArrayList<>();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            Cardinality cardinality = bucket.getAggregations().get("sessions");
            String key = bucket.getKeyAsString() + "\t" + cardinality.getValue();
            int i = 0;
            for (Long toal:totals) {
                if (cardinality.getValue()>= toal) {
                    res.add(i,key);
                    totals.add(i,cardinality.getValue());
                    break;
                }
                i++;
            }
            if (res.isEmpty()) {
                res.add(i,key);
                totals.add(i,cardinality.getValue());
            }
            //如果太长就删除
            if (res.size() > 10&& totals.get(9) > totals.get(10)) {
                for (int j = res.size()-1; j > 9;j--){
                    res.remove(j);
                    totals.remove(j);
                }
            }
        }
        StringBuilder resstr = new StringBuilder();
        int k = 1;
        for (String re : res) {
            resstr.append(k++).append("\t").append(re).append("\n");
        }

        return resstr.toString();
    }
    //第六题
    public String searchSix() throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.size(0);
        //分组
        sourceBuilder.aggregation(AggregationBuilders.histogram("times").field("visitTime").interval(3600)
            .subAggregation(AggregationBuilders.terms("words").field("searchWord").size(3)));
        SearchRequest request = new SearchRequest(index);
        request.source(sourceBuilder);
        Histogram histogram = restHighLevelClient.search(request).getAggregations().get("times");
        StringBuilder res = new StringBuilder();
        histogram.getBuckets().forEach(b ->{
            long time = (long) (Double.parseDouble(b.getKeyAsString())/3600);
            String key = time < 10 ? "0" + time: time+"";
            key += ":00:00\t";
            int j = 1;
            Terms terms = b.getAggregations().get("words");
            for (Terms.Bucket bucket : terms.getBuckets()) {
                String subkey = key + (j++) + "\t" + bucket.getKeyAsString() + "\t" + bucket.getDocCount();
                res.append(subkey).append("\n");
            }

        });
        return res.toString();
    }

    public String searchSeven() throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();
        sourceBuilder.size(0);
        //分组
        sourceBuilder.aggregation(AggregationBuilders.terms("users").size(2000).field("userId").subAggregation(
                AggregationBuilders.cardinality("sessions").field("sessionId")
        ).subAggregation(
                AggregationBuilders.cardinality("words").script(new Script("doc['sessionId'].value + ' ' + doc['searchWord'].value"))
        ));
        SearchRequest request = new SearchRequest(index);
        request.source(sourceBuilder);

        Terms terms = restHighLevelClient.search(request).getAggregations().get("users");

        List<String> res = new ArrayList<>();
        List<Double> totals = new ArrayList<>();
        for (Terms.Bucket bucket : terms.getBuckets()) {
            Cardinality cardinality = bucket.getAggregations().get("sessions");
            Cardinality words = bucket.getAggregations().get("words");
            double v = words.getValue()*1.0/cardinality.getValue();
            String key = bucket.getKeyAsString() + "\t" + cardinality.getValue()+"\t" + v;
            int i = 0;
            for (Double toal:totals) {
                if (v>= toal) {
                    res.add(i,key);
                    totals.add(i,v);
                    break;
                }
                i++;
            }
            if (res.isEmpty()) {
                res.add(i,key);
                totals.add(i,v);
            }
            //如果太长就删除
            if (res.size() > 10&& totals.get(9) > totals.get(10)) {
                for (int j = res.size()-1; j > 9;j--){
                    res.remove(j);
                    totals.remove(j);
                }
            }
        }
        StringBuilder resstr = new StringBuilder();
        int k = 1;
        for (String re : res) {
            resstr.append(k++).append("\t").append(re).append("\n");
        }
        return resstr.toString();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        HttpHost host = new HttpHost(address,9200);
        restClient = RestClient.builder(host).build();
        restHighLevelClient = new RestHighLevelClient(restClient);
    }
}
