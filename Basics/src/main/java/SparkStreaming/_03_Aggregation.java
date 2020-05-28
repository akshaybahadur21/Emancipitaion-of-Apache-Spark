package SparkStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class _03_Aggregation {
    /**
     *
     * Aggregations on a DStream
     *
     * */
    @SuppressWarnings("resource")
    public static void main(String[] args) throws InterruptedException {

        System.setProperty("hadoop.home.dir","C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("Spark Streaming").setMaster("local[*]"); // local[*] means to run spark locally and
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(2));

        JavaReceiverInputDStream<String> inputStream = sc.socketTextStream("localhost", 8989);
        JavaDStream<String> javaDStream = inputStream.map(str -> str); // JavaDStream ~= JavaDStream
        JavaPairDStream<String, Long> pairDStream = javaDStream.mapToPair(msg -> new Tuple2<>(msg.split(",")[0], 1L));
        pairDStream.reduceByKey((a,b)-> a + b).print();
        /**
         * In this case, we will see this job run every 2 second in the spark UI.
         * However, since there is a shuffle happening, each job will actually be 2 jobs
         * */
        sc.start(); //start processing
        sc.awaitTermination(); // until JVM is terminated
    }
}
