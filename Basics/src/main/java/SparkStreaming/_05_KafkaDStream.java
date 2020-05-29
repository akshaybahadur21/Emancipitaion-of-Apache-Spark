package SparkStreaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class _05_KafkaDStream {
    /**
     *
     * Kafka : We will be working with Kafka for generating logs of a video viewing platform
     * In the util package, we have ViewReport Simulator which writes the generated logs to view.logs topic
     * We want get the most watched video in the last hour
     * If someone watches a video for 5 seconds, an event is generated
     *
     * */
    @SuppressWarnings("resource")
    public static void main(String[] args) throws InterruptedException {

        System.setProperty("hadoop.home.dir","C:\\Akshay_GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("Spark Streaming").setMaster("local[*]"); // local[*] means to run spark locally and
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));

        //connect to kafka instead of socket. We don't have support for that out of the box, we must use a plugin
        // We will use spark-streaming-kafka-0-10_2.11

        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("bootstrap.servers", "localhost:9092");
        paramMap.put("key.deserializer", StringDeserializer.class);
        paramMap.put("value.deserializer", StringDeserializer.class);
        paramMap.put("group.id","view.logs_1"); //consumer groups
        paramMap.put("auto.offset.reset","latest"); //consume from latest commit
        paramMap.put("enable.auto.commit",false);

        JavaInputDStream<ConsumerRecord<String, String>> inputData = KafkaUtils.createDirectStream(sc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Arrays.asList("view.logs"), paramMap));
        /**
         * We won't be receiving string but a ConsumerRecord
         * */

        JavaDStream<String> result = inputData.map(item -> item.value());
        JavaPairDStream<Long, String> resultPair = result.mapToPair(item -> new Tuple2<>(item, 5L)).
                reduceByKeyAndWindow((a, b) -> a + b,Durations.minutes(60), Durations.minutes(1)) // aggregating over 60 mins and slide interval of 1 minute
                .mapToPair(item -> item.swap())
                .transformToPair(rdd -> rdd.sortByKey(false));

        //Window size has to be a multiple of batch size
        /**
         * We can specify how often do we want to get logs for the aggregation
         * Right, now that is equal to the batch size -> we get report every second
         * We can use slide interval as described above
         * */
        resultPair.print();

        sc.start(); //start processing
        sc.awaitTermination(); // until JVM is terminated
    }
}
