package SparkStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class _06_StructuredStreaming {
    /**
     *
     * Structured Streaming : Use DataFrames API in streaming manner
     * Check out Structured Streaming API in the documentation
     * Due to continuous processing, we can get latencies of upto 1 millisecond
     * It will almost be continuous stream of data
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
        Dataset<Row> df = session.readStream().
                            format("kafka").
                            option("kafka.bootstrap.servers","localhost:9092").
                            option("subscribe","view.logs")
                            .load();
        df.createOrReplaceTempView("view_log");
        /**
         * Working with kafka and sql is slightly different
         * Kafka sent you consumerRecords in DStream, here it sends you 3 columns
         * <key, value, timestamp>
         * structured streaming explicitly deserialize the values in byte array so we can't use Kafka property to deserealize them
         * we must use cast in sql
         * */
        Dataset<Row> results = session.sql("select CAST(value as STRING) as course_name, sum(5) as seconds_watched from view_log group by course_name order by seconds_watched desc ");
        /**
         * Dataset can only be sinked to a specific sinks
         * Structured Streaming uses a concept of Infinite Tables where aggegations are carry forwarded
         * */
        StreamingQuery query = results.
                                writeStream().
                                format("console").
                                outputMode(OutputMode.Complete()).
                /**
                 * Output Mode : It has 3 options - append, complete or update. Strcutured streaming writes result to ta table
                 * complete : returns entire table; update : only updated rows will be reflected; append : will show only new rows added
                 * */
                                start();
        query.awaitTermination();
    }
}
