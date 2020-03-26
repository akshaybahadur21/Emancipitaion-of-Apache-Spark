package Twytter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwytterProducer {
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String TWYTTER_RAW_TOPIC = "twitter.raw";

    public void produceTweets() throws IOException, InterruptedException, JSONException, ParseException {
        Producer<String, String> kafkaProducer = new KafkaProducer<String, String>(getKafkaProperties());
        Properties prop = new Properties();
        InputStream input = new FileInputStream("/Users/akshay_bahadur/CPE/SparkLearning/src/main/java/Twytter/twitter4j.properties");
        prop.load(input);
        String consumerKey = prop.getProperty("oauth.consumerKey");
        String consumerSecret = prop.getProperty("oauth.consumerSecret");
        String accessToken = prop.getProperty("oauth.accessToken");
        String accessTokenSecret = prop.getProperty("oauth.accessTokenSecret");
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // add some track terms
        endpoint.trackTerms(Lists.newArrayList("Cricket"));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken,
                accessTokenSecret);

        Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();

        // Establish a connection
        client.connect();
        ObjectMapper mapper = new ObjectMapper();
        SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd hh:mm:ss Z YYYY", Locale.ENGLISH);
//        for (int msgRead = 0; msgRead < 10000; msgRead++) {
        while(true) {
            if(!queue.isEmpty()) {
                JSONObject jsonObject = new JSONObject(queue.take());
                TwitterData twitterData = new TwitterData();
                twitterData.setText(String.valueOf(jsonObject.get("text")));
                twitterData.setCreatedAt(String.valueOf(jsonObject.get("timestamp_ms")));
                JSONObject childObject = jsonObject.getJSONObject("user");
                twitterData.setUser(String.valueOf(childObject.get("name")));
                twitterData.setPlace(String.valueOf(childObject.get("location")));
                String record = mapper.writeValueAsString(twitterData);
                ProducerRecord producerRecord = new ProducerRecord(TWYTTER_RAW_TOPIC, record);
                kafkaProducer.send(producerRecord);
            } else {
                Thread.sleep(5000);
            }
        }
//        kafkaProducer.close();
//        client.stop();
    }

    private Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put("metadata.broker.list", BOOTSTRAP_SERVERS);
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
