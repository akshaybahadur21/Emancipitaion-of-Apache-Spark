package SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

public class _01_GettingStarted {
    /**
     *
     * Getting Started with Spark SQL
     * We will use SPARK SESSIONS instead of Java Spark Context in Spqrk SQL
     *
     * */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]")
                                        .config("spark.sql.warehouse","file:///C:/Akshay Github/tmp")
                                        .getOrCreate();
        /**
         *
         * Paremeters Used
         * - appName : Setting name
         * - master : Setting master
         * - config : Pass the configs for Spark SQL -> "spark.sql.warehouse","<Temporary folder for checkpoints>"
         * - getOrCreate : returns the spark session
         * Reminder that the transformations are lazily executed
         *
         * */

        Dataset<Row> dataset = spark.read().option("header", true).csv("src\\main\\resources\\exams\\students.csv");     // for reading dataframes from different sources along with the HEADER
                                                                                                                              // - csv, json, database
        dataset.show();
        System.out.println("Number of rows : " + dataset.count());
        /**
         * Although we are calling count, under the hood, it is map reduce for counting the number of rows.
         * */
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
