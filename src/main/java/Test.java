import api.Movie;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import utils.DBUtil;
import utils.MovieUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Test {
    public static void main(String[] args) throws IOException {

        DBUtil.makeConnection();
        System.out.println("Getting netflix movie...");
        Movie movie = MovieUtil.getMovieById("-aNBSH4BA9zdzX6RVD_r");
        System.out.println("Movie from DB  --> " + movie);


        System.out.println("Inserting a new movie with ...");
        Movie movie2 = new Movie();
        movie2.setCountry("Amman");
        movie2.setShowId("s2");
        movie2.setDirector("oraib edghaim");
        movie2.setReleaseYear(2023L);
        movie2.setRating("PG-10");
        movie2.setDescription("Oraib test");
        movie2.setType("Movie");
        movie2.setTitle("Dead sea");
        movie2.setDuration("30 min");
        movie2.setListedIn("Documentaries");
        movie2.setDateAdded("Jan 16,2022");

        movie2 = MovieUtil.insertMovie(movie2);
        System.out.println("Movie inserted --> " + movie2);


        System.out.println("Changing titile of the movie id : " + "3aec63d6-b006-44c2-8d4d-0f25ac0514a5");
        movie2.setTitle("updated movie");
      //  movie2 = MovieUtil.updateMovieById("3aec63d6-b006-44c2-8d4d-0f25ac0514a5",movie2);
        System.out.println("Movie updated  --> " + movie2);

        System.out.println("Delete the document movie with id : 3aec63d6-b006-44c2-8d4d-0f25ac0514a5");
       // MovieUtil.deleteMovieById("3aec63d6-b006-44c2-8d4d-0f25ac0514a5");

        System.out.println("search based on the document title : ");
        String title ="Dead Sea";
        Optional<Movie> movie3 = MovieUtil.getMovieByTitle(title);
        movie3.ifPresent(m-> System.out.println("The movie is found : "+m));

        Optional<List<Movie>> movie4 = MovieUtil.getMatchedMovies("2012", new String[]{"release_year","date_added","description"});
        System.out.println("/*******************************************************/");
        System.out.println(movie4.get().size());
        movie4.ifPresent(m-> System.out.println("The movie is found for 2012 : "+m));

        Map<String,Map<String,String>> boolCriteria = new HashMap<>();
        Map<String, String> mustMap = new HashMap<String,String>();
        mustMap.put("description","slave");
        boolCriteria.put("must", mustMap);

        Map<String, String> shouldMap = new HashMap<String,String>();
        shouldMap.put("release_year","2012");
        boolCriteria.put("should", shouldMap);

        Optional<List<Movie>> movies = MovieUtil.searchBoolQuery(boolCriteria);
        System.out.println(movies.get().size());
        movies.ifPresent(m-> System.out.println(m));


        String value = "Accompanied by a German bounty hunter, a freed slave named Django travels across America to free his wife from a sadistic plantation owner.";
        Optional<List<Movie>> movies1 = MovieUtil.getMoviesByMinimumPhrases("description",value,8);
        movies1.ifPresent(m-> System.out.println(m));

        Optional<List<Movie>> movies2 = MovieUtil.getMoviesByRatingRange("rating","PG-14","S-18");
        System.out.println("ONLY GET TOP 10 " + movies2.get().size());
        movies2.ifPresent(m-> System.out.println(m));

        /********************************************************************************************************/
        String field = "release_year";
        double min = MovieUtil.getMinAggregation(field);
        System.out.println("The min value for field " + field + " :  "+min);

        Stats stats = MovieUtil.getStatsAggregation(field);
        System.out.println("The max value is " + stats.getMax());
        System.out.println("The sum value is " + stats.getSum());
        System.out.println("The avg value is " + stats.getAvg());


        Map<String,Long> buckets = MovieUtil.getTermsAggBuckets(field);
        System.out.println("The term buckets : ");
        buckets.keySet().stream().forEach(bucket -> System.out.println("Bucket is " + bucket + ", Total doc : " + buckets.get(bucket)));

        Map<String,Long> rangeBuckets = MovieUtil.getRangeAggBuckets(field,2015,2020);
        System.out.println("The range buckets");
        rangeBuckets.keySet().stream().forEach(bucket -> System.out.println("Bucket is " + bucket + ", Total doc : " + rangeBuckets.get(bucket)));

       MovieUtil.printSubAggBuckets(field,"type");
       Map<String,Long> histogramBuckets = MovieUtil.getHistogramBuckets(field,10);
       histogramBuckets.keySet().stream().forEach(bucket -> System.out.println("Bucket is " + bucket + ", Total doc : " + histogramBuckets.get(bucket)));



        DBUtil.closeConnection();
    }
}
