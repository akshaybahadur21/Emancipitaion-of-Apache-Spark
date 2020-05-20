package SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class _10_PivotTables {
    /**
     * Pivot Table(Taken from Excel, supported by Oracle)
     * It can only be used if you have 2 groupings and aggregation
     *
     */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]")
                .config("spark.sql.warehouse", "file:///C:/Akshay Github/tmp")
                .getOrCreate();


        Dataset<Row> dataset = spark.read().option("header", true).csv("src\\main\\resources\\biglog.txt.txt");
        dataset.createOrReplaceTempView("logging_big_table");


        /**
         * Each pair from the 'result' is a group of level and month followed by the count for that group.
         * We will create a 2D matrix of level, month
         * In this case, out matrix will be 5 logging levels and 12 months (60 cells)
         * This can't be done directly from SQL so we will use java apis from _09_DataFrames
         *
         * First grouping has to be done on the row column -> pivot on column -> aggregation
         * eg ->  dataset.groupBy("level").pivot("month").count();
         * */

        dataset = dataset.select(col("level"),
                date_format(col("datetime"),"MMMM").alias("month"),
                date_format(col("datetime"),"M").alias("month_num").cast(DataTypes.IntegerType));


        dataset.groupBy("level").pivot("month").count().show(60,false);

        //dataset.groupBy("level").pivot("monthnum").count(); for ordering by month

        List<Object> columns = new ArrayList<>();
        columns.add("January");
        columns.add("February");
        columns.add("March");
        columns.add("April");
        columns.add("May");
        columns.add("June");
        columns.add("July");
        columns.add("August");
        columns.add("September");
        columns.add("October");
        columns.add("November");
        columns.add("December");

        dataset.groupBy("level").pivot("month",columns).count().show(60, false);

        //What if you get a 0 value for a certail cell, spark will show it as null
        // You can use na() function

        dataset.groupBy("level").pivot("month",columns).count().na().fill(0).show(60, false);


        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
