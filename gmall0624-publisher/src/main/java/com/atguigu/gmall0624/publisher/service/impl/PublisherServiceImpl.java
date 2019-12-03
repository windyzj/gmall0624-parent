package com.atguigu.gmall0624.publisher.service.impl;

import com.atguigu.gmall0624.common.constant.GmallConstant;
import com.atguigu.gmall0624.publisher.mapper.DauMapper;
import com.atguigu.gmall0624.publisher.mapper.OrderMapper;
import com.atguigu.gmall0624.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        Long dauTotal = dauMapper.getDauTotal(date);
        return dauTotal;
    }

    @Override
    public Map getDauHour(String date) {
        List<Map> dauHourMapList = dauMapper.getDauHour(date);  // [{"loghour":9,"ct",435},"loghour":11,"ct",790}]
        Map dauHourMap=new HashMap();//{"9":435, "11":790}

        for (Map map : dauHourMapList) {
            dauHourMap.put( map.get("loghour"),map.get("ct"));
        }

        return dauHourMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        Double orderAmount = orderMapper.selectOrderAmount(date);
        return orderAmount;
    }

    @Override
    public Map getOrderAmountHourMap(String date) {
        List<Map> mapList = orderMapper.selectOrderAmountHour(date);
        Map hourAmountMap=new HashMap();
        for (Map colValMap : mapList) {
            hourAmountMap.put ( colValMap.get("CREATE_HOUR"),colValMap.get("ORDER_AMOUNT"));
        }

        return hourAmountMap;
    }

    @Override
    public Map getSaleDetail(String dt, String keyword, int pageNo, int pageSize) {

        String query="{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"dt\": \"2019-12-02\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\":  {\n" +
                "        \"match\":{\n" +
                "          \"sku_name\":\n" +
                "            { \"query\":\"小米\",  \"operator\":\"and\" }\n" +
                "        }\n" +
                "      }\n" +
                "      \n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupby_gender\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"user_gender\",\n" +
                "        \"size\": 2\n" +
                "      }\n" +
                "    },\n" +
                "    \"groupby_age\":{\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"user_age\",\n" +
                "        \"size\": 120\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"from\": 0\n" +
                " ,\"size\": 20  \n" +
                "}";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //查询
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt",dt));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合
        TermsBuilder genderAggs = AggregationBuilders.terms("groupby_gender").field("user_gender").size(2);
        TermsBuilder ageAggs = AggregationBuilders.terms("groupby_age").field("user_age").size(112);

        searchSourceBuilder.aggregation(genderAggs);
        searchSourceBuilder.aggregation(ageAggs);

        //分页
        //页码转行号
        int rowNo = (pageNo - 1) * pageSize;
        searchSourceBuilder.from(rowNo);
        searchSourceBuilder.size(pageSize);


        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE).addType("_doc").build();

        Map resultMap=new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //总数
            Long total = searchResult.getTotal();

            //明细
            List<Map> detailList=new ArrayList<>();
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            for (SearchResult.Hit<Map, Void> hit : hits) {
                detailList.add(hit.source) ;
            }
            //聚合结果
            //性别聚合
            List<TermsAggregation.Entry> genderBuckets = searchResult.getAggregations().getTermsAggregation("groupby_gender").getBuckets();
            Map genderMap=new HashMap();
            for (TermsAggregation.Entry bucket : genderBuckets) {
                genderMap.put(  bucket.getKey(),bucket.getCount());
            }
             //年龄聚合
            List<TermsAggregation.Entry> ageBuckets = searchResult.getAggregations().getTermsAggregation("groupby_age").getBuckets();
            Map ageMap=new HashMap();
            for (TermsAggregation.Entry bucket : ageBuckets) {
                ageMap.put(  bucket.getKey(),bucket.getCount());
            }


            resultMap.put("total",total);
            resultMap.put("detail",detailList);
            resultMap.put("genderAgg",genderMap);
            resultMap.put("ageAgg",ageMap);


        } catch (IOException e) {
            e.printStackTrace();
        }

        return resultMap;
    }
}
