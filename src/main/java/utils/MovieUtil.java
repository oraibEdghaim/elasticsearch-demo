package utils;

import api.Movie;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
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
    public static Movie insertMovie(Movie movie){
        try {
            String movieJson = objectMapper.writeValueAsString(movie);
            IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, UUID.randomUUID().toString())
                    .source(movieJson,XContentType.JSON);
            IndexResponse response = restHighLevelClient.index(indexRequest);
        } catch(ElasticsearchException e) {
            System.out.println(e.getMessage());
        } catch (java.io.IOException ex){
            System.out.println(ex.getMessage());
        }
        return movie;
    }

    public static Movie getMovieById(String id){
        GetRequest getPersonRequest = new GetRequest(INDEX,TYPE, id);
        GetResponse getResponse = null;
        try {
            getResponse = restHighLevelClient.get(getPersonRequest, RequestOptions.DEFAULT);
        } catch (java.io.IOException e){
            System.out.println(e.getMessage());
        }
        Movie movie =  getResponse != null ? objectMapper.convertValue(getResponse.getSourceAsMap(), Movie.class) : null;
        return movie;
    }
    public static Movie updateMovieById(String id, Movie movie){
        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, id).fetchSource(true);    // Fetch Object after its update
        try {
            String movieJson = objectMapper.writeValueAsString(movie);
            updateRequest.doc(movieJson, XContentType.JSON);
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest);
            return objectMapper.convertValue(updateResponse.getGetResult().sourceAsMap(), Movie.class);
        }catch (JsonProcessingException e){
            System.out.println(e.getMessage());
        } catch (java.io.IOException e){
            System.out.println(e.getMessage());
        }
        System.out.println("Unable to update movie");
        return null;
    }

    public static void deleteMovieById(String id) {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest);
        } catch (java.io.IOException e){
            System.out.println(e.getMessage());
        }
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
       Map<String,Long> bucketResults =  new HashMap<String, Long>();
       for (Terms.Bucket bucket : buckets.getBuckets()) {
           String bucketKey = bucket.getKeyAsString();
           long totalDocs = bucket.getDocCount();
           bucketResults.put(bucketKey,totalDocs);
       }
       return bucketResults;
   }
}
