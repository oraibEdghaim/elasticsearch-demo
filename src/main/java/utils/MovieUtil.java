package utils;

import api.Movie;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;

public class MovieUtil {

    private static ObjectMapper objectMapper = new ObjectMapper();
    private static final String INDEX = "netflix";
    private static final String TYPE = "_doc";
    private static RestHighLevelClient restHighLevelClient;

    static {
        restHighLevelClient = DBUtil.makeConnection();
    }
    private MovieUtil(){

    }
    public static RestStatus insertMovie(Movie movie) throws IOException {
            String movieJson = objectMapper.writeValueAsString(movie);
            IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, UUID.randomUUID().toString())
                    .source(movieJson,XContentType.JSON);
            IndexResponse response = restHighLevelClient.index(indexRequest);
            return response.status();
    }

    public static Movie getMovieById(String id) throws IOException {
        GetRequest getPersonRequest = new GetRequest(INDEX,TYPE, id);
        GetResponse getResponse = null;
        getResponse = restHighLevelClient.get(getPersonRequest, RequestOptions.DEFAULT);
        return getResponse != null ? objectMapper.convertValue(getResponse.getSourceAsMap(), Movie.class) : null;
    }
    public static Movie updateMovieById(String id, Movie movie) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, id).fetchSource(true);    // Fetch Object after its update
            String movieJson = objectMapper.writeValueAsString(movie);
            updateRequest.doc(movieJson, XContentType.JSON);
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest);
            return objectMapper.convertValue(updateResponse.getGetResult().sourceAsMap(), Movie.class);
    }

    public static RestStatus deleteMovieById(String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest);
        return deleteResponse.status();
    }

    public static Optional<Movie> getMovieByTitle(String title) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        // Can use the method matchPhraseQuery from QueryBuilder or using thr customized query builder class
        searchBuilder.query(QueryBuilders.matchPhraseQuery("title",title.trim()));
        MatchPhraseQueryBuilder matchPhraseQueryBuilder = new MatchPhraseQueryBuilder("title",title.trim());
        searchBuilder.query(matchPhraseQueryBuilder);

        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.source(searchBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        Movie movie =  response != null ? objectMapper.convertValue(response.getHits().getAt(0).getSourceAsMap(), Movie.class) : null;
        return Optional.ofNullable(movie);
    }

    public static Optional<List<Movie>> getMatchedMovies(String txt, String[] fieldNames) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
       //searchBuilder.query(QueryBuilders.multiMatchQuery(txt, fieldNames));
        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(txt,fieldNames);
        searchBuilder.query(multiMatchQueryBuilder);

        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.source(searchBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);

        List<Movie> movies = new ArrayList<Movie>();
        if(!isNull(response)){
            movies = Arrays.asList(response.getHits().getHits()).stream().map(hit -> defineHitResponse(hit)).collect(Collectors.toList());
        }

        return Optional.of(movies);
    }
    public static Optional<List<Movie>> searchBoolQuery(Map<String, Map<String,String>> boolCriteria) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();

        boolCriteria.keySet().stream().forEach(key -> applyBoolQuery(key,boolCriteria.get(key),searchBuilder,boolQuery));

        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.source(searchBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        System.out.println("The number of movies is : " + response.getHits().getTotalHits());
        List<Movie> movies = new ArrayList<Movie>();
        if(!isNull(response)){
            movies = Arrays.asList(response.getHits().getHits()).stream().map(hit -> defineHitResponse(hit)).collect(Collectors.toList());
        }
        return Optional.ofNullable(movies);
    }
    private static void applyBoolQuery(String key,Map<String,String> boolCriteria,SearchSourceBuilder searchBuilder,BoolQueryBuilder boolQuery) {
        switch (key){
            case "must":
                applyMust(searchBuilder,boolQuery,boolCriteria);
                break;
            case "should":
               applyShould(searchBuilder,boolQuery,boolCriteria);
                break;
            default:
                throw new IllegalArgumentException("The bool query type is not defined well");
        }

    }
    private static void applyMust( SearchSourceBuilder searchBuilder,BoolQueryBuilder boolQuery,Map<String,String>mustCriteria) {
        String key = mustCriteria.keySet().stream().findFirst().get();
        boolQuery.must(QueryBuilders.matchQuery(key,mustCriteria.get(key)));
        searchBuilder.query(boolQuery);
    }
    private static void applyShould(SearchSourceBuilder searchBuilder, BoolQueryBuilder boolQuery, Map<String, String> shouldCriteria) {
        String key = shouldCriteria.keySet().stream().findFirst().get();
        boolQuery.should(QueryBuilders.matchQuery(key,shouldCriteria.get(key)));
        searchBuilder.query(boolQuery);
    }
    private static Movie defineHitResponse(SearchHit hit) {
        return objectMapper.convertValue(hit.getSourceAsMap(),Movie.class);
    }

    public static Optional<List<Movie>> getMoviesByMinimumPhrases(String field,String value, int minimumShouldMatch) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        MatchQueryBuilder matchBuilder = new MatchQueryBuilder(field,value);
        matchBuilder.minimumShouldMatch(String.valueOf(minimumShouldMatch));
        searchBuilder.query(matchBuilder);

        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.source(searchBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        System.out.println("The number of movies which is matched the "+field+" : "+ value + ", " + response.getHits().getTotalHits());
        List<Movie> movies = new ArrayList<Movie>();
        if(!isNull(response)){
            movies = Arrays.asList(response.getHits().getHits()).stream().map(hit -> defineHitResponse(hit)).collect(Collectors.toList());
        }
        return Optional.ofNullable(movies);

    }

    public static Optional<List<Movie>> getMoviesByRatingRange(String field, Object gte, Object lte) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        RangeQueryBuilder rangeBuilder = new RangeQueryBuilder(field);
        rangeBuilder.gte(gte);
        rangeBuilder.lte(lte);
        searchBuilder.query(rangeBuilder);

        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.source(searchBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        System.out.println("The number of movies with ranges gte and lte "+field+" : " + ", " + response.getHits().getTotalHits());
        List<Movie> movies = new ArrayList<Movie>();
        if(!isNull(response)){
            movies = Arrays.asList(response.getHits().getHits()).stream().map(hit -> defineHitResponse(hit)).collect(Collectors.toList());
        }
        return Optional.ofNullable(movies);

    }
    public static double getMinAggregation(String field) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        MinAggregationBuilder minBuilder = AggregationBuilders.min("agg").field(field);
       // MinAggregationBuilder minBuilder = new MinAggregationBuilder(field);
        searchBuilder.size(0);
        searchBuilder.aggregation(minBuilder);

        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.source(searchBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        Min agg = response.getAggregations().get("agg");
        return agg.getValue();
    }
   public static Stats getStatsAggregation(String field) throws IOException {
       SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
       StatsAggregationBuilder aggregation = AggregationBuilders.stats("agg").field(field);
       searchBuilder.aggregation(aggregation);
       searchBuilder.size(0);
       SearchRequest searchRequest = new SearchRequest(INDEX);
       searchRequest.source(searchBuilder);
       SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
       Stats stats = response.getAggregations().get("agg");
       return stats;
   }
   public static Map<String, Long> getTermsAggBuckets(String field) throws IOException {
       SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
       String aggregationName ="ByField";
       searchBuilder.aggregation(AggregationBuilders.terms(aggregationName).field(field));
       searchBuilder.size(0);

       SearchRequest searchRequest = new SearchRequest(INDEX);
       searchRequest.source(searchBuilder);

       SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
       Aggregations aggregations = response.getAggregations();
       Terms buckets = aggregations.get(aggregationName);
       Map<String,Long> bucketResults =  new HashMap<>();
       for (Terms.Bucket bucket : buckets.getBuckets()) {
           String bucketKey = bucket.getKeyAsString();
           long totalDocs = bucket.getDocCount();
           bucketResults.put(bucketKey,totalDocs);
       }
       return bucketResults;
   }
    public static Map<String, Long>  getRangeAggBuckets(String field, double from, double to) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        String aggregationName ="RangeAgg";
        searchBuilder.aggregation(AggregationBuilders.range(aggregationName).field(field).addRange(from,to));
        searchBuilder.size(0);

        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.source(searchBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        Aggregations aggregations = response.getAggregations();
        Range buckets = aggregations.get(aggregationName);
        Map<String,Long> bucketResults =  new HashMap<>();
        buckets.getBuckets().forEach(bucket -> bucketResults.put(bucket.getKeyAsString(),bucket.getDocCount()));

        return bucketResults;
    }

    public static void printSubAggBuckets (String field, String subField) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        String aggregationName = "SubAggByRange";
        RangeAggregationBuilder rangeBuilder = new RangeAggregationBuilder(aggregationName);
        org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range range1 = new RangeAggregator.Range("2012-2015",2012.0,2015.0);
        org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range range2 = new RangeAggregator.Range("2016-2020",2016.0,2020.0);
        org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range range3 = new RangeAggregator.Range("2020",2020.0,null);
        rangeBuilder.field(field).addRange(range1);
        rangeBuilder.field(field).addRange(range2);
        rangeBuilder.field(field).addRange(range3);

        String subAggregationName = "subAggByTerm";
        TermsAggregationBuilder termBuilder = AggregationBuilders.terms(subAggregationName).field(subField);

        rangeBuilder.subAggregation(termBuilder);
        searchBuilder.aggregation(rangeBuilder);
        searchBuilder.size(0);

        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.source(searchBuilder);

        SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        Aggregations aggregations = response.getAggregations();
        Range buckets = aggregations.get(aggregationName);
        System.out.println("Result of printSubAggBuckets funtionality");
        for (Range.Bucket bucket : buckets.getBuckets()) {
            String bucketKey = bucket.getKeyAsString();
            long totalDocs = bucket.getDocCount();
            Terms termBuckets = bucket.getAggregations().get(subAggregationName);
            System.out.println("Bucket key is " + bucketKey );
            System.out.println("Bucket total documents is " + totalDocs);
            System.out.println("Sub aggregation buckets by term");
            termBuckets.getBuckets().forEach(termBucket -> System.out.println("Key : " + termBucket.getKeyAsString() + ", Total documents : " +termBucket.getDocCount()));
        }
    }
    public static Map<String, Long> getHistogramBuckets(String field, double interval) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        String histogramAggregation = "histogramAggregation";
        HistogramAggregationBuilder histogramBuilder = new HistogramAggregationBuilder(histogramAggregation);
        histogramBuilder.field(field).interval(interval);
        searchBuilder.aggregation(histogramBuilder);

        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.source(searchBuilder);
        System.out.println("Histogram Buckets : " + " Field : " + field + ", Interval : " + interval);
        SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        Aggregations aggregations = response.getAggregations();
        Histogram histogramResults = aggregations.get(histogramAggregation);
        Map<String,Long> buckets = new HashMap<>();
        histogramResults.getBuckets().forEach(bucket -> buckets.put(bucket.getKeyAsString(),bucket.getDocCount()));
        return buckets;
    }
    // pagination sections :
    public static Optional<List<Movie>> getPageResult(int from, int size) throws IOException {
        SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
        searchBuilder.query(QueryBuilders.matchAllQuery());
        searchBuilder.from(from);
        searchBuilder.size(size);

        SearchRequest searchRequest = new SearchRequest(INDEX);
        searchRequest.source(searchBuilder);
        System.out.println("Pagination based on from and size parameter : from : "+from + ", size : "+size);
        SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        List<Movie> movies = new ArrayList<Movie>();
        if(!isNull(response)){
            movies = Arrays.asList(response.getHits().getHits()).stream().map(hit -> defineHitResponse(hit)).collect(Collectors.toList());
        }
        return Optional.ofNullable(movies);
    }

}
