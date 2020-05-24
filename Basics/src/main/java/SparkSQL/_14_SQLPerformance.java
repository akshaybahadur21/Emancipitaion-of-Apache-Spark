package SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class _14_SQLPerformance {
    /**
     * Performance : Since underneath all of this is running on RDDs so all the performance from Spark Core works here
     *      - Avoid unnecessary shuffling (Since we are using SQL, Spark is responsible for generating Java equivalent for the same therefore, Performance is abstracted from us)
     *      - Spark SQL performs less efficiently than equivalent Java APi
     */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]")
                .config("spark.sql.warehouse", "file:///C:/Akshay Github/tmp")
                .getOrCreate();
//        spark.conf().set("spark.sql.shuffle.partitions", 12);

        Dataset<Row> dataset = spark.read().option("header", true).csv("src\\main\\resources\\biglog.txt.txt");
        dataset.createOrReplaceTempView("logging_table");

        /**
         * The Spark SQL Api and Spark SQL Java Api use Whole stage codegen which is responsible for compiling multiple operators
         * into a single java function. The whole stage codegen improved the speed of Spark SQL by 10 times from previous.
         * Spark UI will generate less information with Spark SQL since you are using a lot of abstraction
         * We can't speed up whole Stage codegen since it is a black box
         * So what we look for in the UI
         *      - Less number of shuffles (less stages)
         *      - How much data is being exchanges between stages (Pivot creates stages but the data transferred between stages is less)
         *
         * To get good information, go to the SQL tab in Spark UI
         * */
        Dataset<Row> results = spark.sql
                ("select level, date_format(datetime,'MMMM') as month, count(1) as total " +
                        "from logging_table group by level, month order by cast(first(date_format(datetime,'M')) as int), level");
        results.show();

//        dataset = dataset.select(col("level"),
//                date_format(col("datetime"),"MMMM").alias("month"),
//                date_format(col("datetime"),"M").alias("month_num").cast(DataTypes.IntegerType));
//
//        dataset = dataset.groupBy(col("level"), col("month"), col("month_num")).count().as("Total").orderBy("month_num");
//        dataset = dataset.drop("month_num");
//
//        dataset.show();

        /**
         * Spark SQL v/s Spark SQL Java Api (for doing it we comment either code and see)
         * Same SQL code takes about 60 seconds and JAVA Api takes 20 seconds
         * WHY -> Hash Aggregations
         * */

        /**
         * spark.sql.shuffle.partitions
         * when grouping is done, data is shuffled around -> some partitions of data are created for shuffle -> each partition will have a task assigned to that partition.
         * Spark SQL needs the information of how many partitions to use.
         * If we check the UI, we will find 200 tasks (partition) but most of those partition will have no data
         * This is because we have done some groupings which means that we have grouped by month(12) and log level(5).
         * Therefore, a total of 60 keys in output (140 partitions will be idle)
         * In spark streaming, this hurts us a lot
         *
         * Fix : spark.conf().set("spark.sql.shuffle.partitions", 60)
         * */
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
