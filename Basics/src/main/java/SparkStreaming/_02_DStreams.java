package SparkStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class _02_DStreams {
    /**
     *
     * DStream : Discretized Stream
     * Flow is given below
     * Input Data -> [Spark Streaming] -> {Batches of i/p data} -> [Spark Engine] -> {Batches of processed data} -> <FURTHER PROCESSING>
     *
     * We can specify the batch interval eg : every second, we create a batch of i/p stream and send it to spark engine
     * It feels that we are only working on 1 RDD. However, under the hood that complication is abstracted away in DStream
     *
     * */
    @SuppressWarnings("resource")
    public static void main(String[] args) throws InterruptedException {
        /**
         * We need some kind of streaming data source
         *      - netcat can be used(unix) : stream of data sent to a socket
         *      - Apache Kafka
         * */
        System.setProperty("hadoop.home.dir","C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("Spark Streaming").setMaster("local[*]"); // local[*] means to run spark locally and
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(30));

        //kafka will require a special plugin
        JavaReceiverInputDStream<String> inputStream = sc.socketTextStream("localhost", 8989);
        JavaDStream<String> result = inputStream.map(str -> str); // JavaDStream ~= JavaDStream
        result.print();
        // at the end of each batch, results will be presented with a time stamp

        sc.start(); //start processing
        sc.awaitTermination(); // until JVM is terminated
    }
}
