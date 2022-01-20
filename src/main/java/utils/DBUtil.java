package utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class DBUtil {

    //The config parameters for the connection
    private static final String HOST = "localhost";
    private static final int PORT_ONE = 9200;
    private static final int PORT_TWO = 9201;
    private static final String SCHEME = "http";

    private static RestHighLevelClient restHighLevelClient;

    private DBUtil(){

    }
    public static synchronized RestHighLevelClient makeConnection() {

        if(restHighLevelClient == null) {
             restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(HOST, PORT_ONE, SCHEME),
                            new HttpHost(HOST, PORT_TWO, SCHEME)));
        }

        return restHighLevelClient;
    }

    public static synchronized void closeConnection() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }

    public static boolean createMessagnerIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("messenger");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);

        if(exists) {
            return false;
        }

        CreateIndexRequest indexRequest = new CreateIndexRequest("messenger");
        Map<String, Object> message = new HashMap<>();
        message.put("type", "text");
        Map<String, Object> time = new HashMap<>();
        time.put("type", "date");
        Map<String, Object> sender = new HashMap<>();
        sender.put("type", "text");
        Map<String, Object> receiver = new HashMap<>();
        receiver.put("type", "text");


        Map<String, Object> properties = new HashMap<>();
        properties.put("message", message);
        properties.put("date", time);
        properties.put("sender", sender);
        properties.put("receiver", receiver);

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        indexRequest.mapping(mapping);


        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(indexRequest, RequestOptions.DEFAULT);

        boolean acknowledged = createIndexResponse.isAcknowledged();
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();

        return  acknowledged && shardsAcknowledged;

    }

}
