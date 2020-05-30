package SparkStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class _07_SSWindowing {
    /**
     *
     * Windowing
     *+------------------------------------------+-----------------------------------+---------------+
     * |window                                    |course_name                        |seconds_watched|
     * +------------------------------------------+-----------------------------------+---------------+
     * |[2020-05-31 02:57:00, 2020-05-31 02:58:00]|Java Web Development               |45             |
     * |[2020-05-31 02:58:00, 2020-05-31 02:59:00]|Docker Module 2 for Java Developers|455            |
     * |[2020-05-31 02:57:00, 2020-05-31 02:58:00]|Spring JavaConfig                  |85             |
     * |[2020-05-31 02:57:00, 2020-05-31 02:58:00]|NoSQL Databases                    |20             |
     * |[2020-05-31 02:58:00, 2020-05-31 02:59:00]|Spring Framework Fundamentals      |535            |
     * |[2020-05-31 02:57:00, 2020-05-31 02:58:00]|Docker Module 2 for Java Developers|240            |
     * |[2020-05-31 02:58:00, 2020-05-31 02:59:00]|Git                                |5              |
     * |[2020-05-31 02:57:00, 2020-05-31 02:58:00]|Spring Framework Fundamentals      |270            |
     * |[2020-05-31 02:58:00, 2020-05-31 02:59:00]|Spring Security Module 3           |505            |
     * |[2020-05-31 02:58:00, 2020-05-31 02:59:00]|NoSQL Databases                    |15             |
     * |[2020-05-31 02:58:00, 2020-05-31 02:59:00]|Java Web Development               |65             |
     * |[2020-05-31 02:57:00, 2020-05-31 02:58:00]|Docker for Java Developers         |10             |
     * |[2020-05-31 02:57:00, 2020-05-31 02:58:00]|Java Fundamentals                  |660            |
     * |[2020-05-31 02:58:00, 2020-05-31 02:59:00]|Hibernate and JPA                  |575            |
     * |[2020-05-31 02:58:00, 2020-05-31 02:59:00]|Docker for Java Developers         |20             |
     * |[2020-05-31 02:57:00, 2020-05-31 02:58:00]|Java Advanced Topics               |350            |
     * |[2020-05-31 02:58:00, 2020-05-31 02:59:00]|Groovy Programming                 |45             |
     * +------------------------------------------+-----------------------------------+---------------+
     *
     * The timestamp of the window comes from the actual event when it is received by kafka.
     * Due to network issues, kafka can cause delays but spark ensures that the event is recorded against the correct window.
     * However, in doing so, Spark has to store intermediate data in memory so that it can insert the late event in the correct window.
     * To target this, Spark introduced Water Marking
     * Data can be discarded if it is received late by certain number of minutes using water marking.
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
