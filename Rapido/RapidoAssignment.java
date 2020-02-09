package rapido;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.uber.h3core.H3Core;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple7;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Please find the DE assignment given below:
 * Link to dataset: https://drive.google.com/a/rapido.bike/file/d/19Vw9Vf6nGpdRKb8qAK9qxmO8jBGiTWWx/view?usp=drivesdk
 * 1. Import this dataset into a Kafka topic.
 * 2. Process this stream to transform the latitude/longitude values into a hex id of resolution 7 (https://eng.uber.com/h3/, https://github.com/uber/h3)
 * 3. Once this is done, we would need hourly, daily, weekly and monthly averages of aggregated counts of each hex_id into different topics.
 * 4. You would be using some stream processing tool to do this (Flink or Storm, or whatever you're comfortable with using windowing operators).
 * Can you please let us know when you can complete the assignment and submit.
 * Have copied the data team who can help you if you have any questions around this.
 * This assignment is relatively open ended so reach out if you have any questions. Would be good if you could share progress with us when possible.
 */

/*
* My Notes
* rapido.akshay = 285,359
* rapido.akshay.trans = 285,359
*
*
* */

public class RapidoAssignment {
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static int RESOLUTION = 7;
    private final static String TOPIC_NAME = "rapido.akshay";

    public static void main(String[] args) throws InterruptedException, StreamingQueryException, IOException {
        RapidoAssignment rapidoAssignment = new RapidoAssignment();
//        rapidoAssignment.transformRapidoStreaming();
        rapidoAssignment.countByWindowStreaming();
    }

    private void countByWindowStreaming() throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Rapido").setMaster("local");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(60000));
        streamingContext.checkpoint("/Users/akshay_bahadur/dummy/");
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
                        ConsumerStrategies.<String, String>Subscribe(Arrays.asList("rapido.akshay.trans"), kafkaParams));

        JavaDStream<Long> longJavaDStream = messages.countByWindow(new Duration(60000), new Duration(60000)); //1 Minute
        longJavaDStream.foreachRDD(rdd ->{
            rdd.foreachPartition(p->{
                while (p.hasNext())
                    System.out.println(p.next());
            });
        });
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private void transformRapidoStreaming() throws InterruptedException, IOException {

        SparkConf conf = new SparkConf().setAppName("Rapido").setMaster("local");
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
                        ConsumerStrategies.<String, String>Subscribe(Arrays.asList(TOPIC_NAME), kafkaParams));

        messages.foreachRDD(rdd -> {
            JavaRDD<String> stringJavaRDD = rdd.map(new RapidRDDToStringMapper());
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

    public void produceCsvToKafka() {
        ObjectMapper mapper = new ObjectMapper();
        Producer<String, String> kafkaProducer = new KafkaProducer<String, String>(getKafkaProperties());
        Pattern pattern = Pattern.compile(",");
        try (BufferedReader in = new BufferedReader(new FileReader("/Users/akshay_bahadur/CPE/SparkLearning/src/main/java/rapido/ct_rr.csv"));) {
            List<RapidoData> players = in.lines().skip(1).map(line -> {
                String[] x = pattern.split(line);
                RapidoData data = new RapidoData(x[0], x[1], x[2], x[3], x[4], x[5]);
                String jsonNode = null;
                try {
                    jsonNode = mapper.writeValueAsString(data);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                if (jsonNode != null) {
                    ProducerRecord producerRecord = new ProducerRecord(TOPIC_NAME, jsonNode);
                    kafkaProducer.send(producerRecord);
                }
                return new RapidoData(x[0], x[1], x[2], x[3], x[4], x[5]);
            }).collect(Collectors.toList());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Publish Successful");
    }

    public static Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }


    private static class RapidRDDToStringMapper implements Function<ConsumerRecord<String, String>, String> {
        @Override
        public String call(ConsumerRecord<String, String> record) throws Exception {
            ObjectMapper Obj = new ObjectMapper();
            RapidoData data = new Gson().fromJson(record.value(), RapidoData.class);

            RapidoTransformedData rapidoTransformedData = new RapidoTransformedData();
            rapidoTransformedData.setNumber(data.getNumber());
            rapidoTransformedData.setTs(data.getTs());
            rapidoTransformedData.setPick_lat(data.getPick_lat());
            rapidoTransformedData.setPick_lng(data.getPick_lng());
            rapidoTransformedData.setDrop_lat(data.getDrop_lat());
            rapidoTransformedData.setDrop_lng(data.getDrop_lng());
            rapidoTransformedData.setNumber(data.getNumber());
            rapidoTransformedData.setPickHex(SerializeHex.getSerializeHex(data.getPick_lat(), data.getPick_lng(), RESOLUTION));
            rapidoTransformedData.setDropHex(SerializeHex.getSerializeHex(data.getDrop_lat(), data.getDrop_lng(), RESOLUTION));
            return Obj.writeValueAsString(rapidoTransformedData);
        }
    }
}
