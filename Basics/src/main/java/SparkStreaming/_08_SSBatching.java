package SparkStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class _08_SSBatching {
    /**
     *
     * Batching in Structured Streaming
     * We have noticed long batches in Structured Streaming
     * In structured streaming, stop thinking about batches
     *
     * Structured Streaming works via Trigger
     * Trigger : we can specify a trigger when a structured streaming job will go ahead
     *
     * The main concept behind Strcutured Streaming batching is that there ar eno batches, As soon as one batch, it is going to go ahead
     * and work on the remaining set of data
     *
     * */
    @SuppressWarnings("resource")
    public static void main(String[] args) throws InterruptedException, StreamingQueryException {

        System.setProperty("hadoop.home.dir","C:\\Akshay_GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder().
                                master("local[*]").
                                appName("Structured Streaming").
                                getOrCreate();
        session.conf().set("spark.sql.shuffle.partitions", "10"); //only partition the data to 10 partitions
        Dataset<Row> df = session.readStream().
                            format("kafka").
                            option("kafka.bootstrap.servers","localhost:9092").
                            option("subscribe","view.logs")
                            .load();
        df.createOrReplaceTempView("view_log");

        Dataset<Row> results = session.sql("select  window, CAST(value as STRING) as course_name, sum(5) as seconds_watched from view_log group by window(timestamp, '1 minute'), course_name ");

        StreamingQuery query = results.
                                writeStream().
                                format("console").
                                outputMode(OutputMode.Update()).
                                option("truncate", false).
                                option("numRows", 50).
                                start();
        query.awaitTermination();
    }
}
