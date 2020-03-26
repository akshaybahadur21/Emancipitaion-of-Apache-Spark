package Twytter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.codehaus.jettison.json.JSONException;

import java.io.*;
import java.text.ParseException;
import java.util.*;

public class Twytter implements Serializable {
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String TWYTTER_RAW_TOPIC = "twitter.raw";

    public static void main(String[] args) throws InterruptedException, IOException, JSONException, ParseException {
        Twytter twytter = new Twytter();
        TwytterProducer producer = new TwytterProducer();
        producer.produceTweets();
//        twytter.streamDataFromTwitter();
    }

    private void transformRapidoStreaming() throws InterruptedException, IOException {

        SparkConf conf = new SparkConf().setAppName("Twytter").setMaster("local");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(100000));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        kafkaParams.put("group.id", "akshay_check");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", "false");

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(Arrays.asList(TWYTTER_RAW_TOPIC), kafkaParams));

        messages.foreachRDD(rdd -> {
            JavaRDD<String> stringJavaRDD = rdd.map(new TwitterDataToStringMapper());
            stringJavaRDD.foreachPartition(p -> {
                Producer<String, String> kafkaProducer = new KafkaProducer<String, String>(getKafkaProperties());
                while (p.hasNext()) {
                    ProducerRecord producerRecord = new ProducerRecord("rapido.akshay.trans", p.next());
                    kafkaProducer.send(producerRecord);
                }
            });
        });
        streamingContext.start();
        streamingContext.awaitTermination();
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

    private class TwitterDataToStringMapper implements Function<ConsumerRecord<String, String>, String>{
        @Override
        public String call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
            return null;
        }
    }
}
