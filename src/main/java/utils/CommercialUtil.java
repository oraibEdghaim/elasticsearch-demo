package utils;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CommercialUtil {

    private static final String INDEX = "ecommerce_data_test";
    private static RestHighLevelClient restHighLevelClient;

    private CommercialUtil(){

    }
    static {
        restHighLevelClient = DBUtil.makeConnection();
    }

   public static Map<String,Long> getDateHistogramBucket(String field, DateHistogramInterval interval) throws IOException {
       SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
       String aggregationName = "dateHistogram";
       DateHistogramAggregationBuilder histogramAggregationBuilder = new DateHistogramAggregationBuilder(aggregationName);
       histogramAggregationBuilder.field(field).dateHistogramInterval(interval);
       searchBuilder.aggregation(histogramAggregationBuilder);

       SearchRequest searchRequest = new SearchRequest(INDEX);
       searchRequest.source(searchBuilder);

       System.out.println("The commerce data based on date histogram aggregation functionality");
       SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
       Aggregations aggregations = response.getAggregations();
       Histogram dateHistogram = aggregations.get(aggregationName);
       Map<String,Long> dateHistogramBuckets = new HashMap<>();
       dateHistogram.getBuckets().forEach(bucket-> dateHistogramBuckets.put(bucket.getKeyAsString(),bucket.getDocCount()));
       return dateHistogramBuckets;
   }

}
