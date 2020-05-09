package SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class _05_GroupingAndAggregation {
    /**
     *
     * Grouping and Aggregation
     * Explore SQL syntax for grouping and aggregation
     *
     * */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]")
                                        .config("spark.sql.warehouse","file:///C:/Akshay Github/tmp")
                                        .getOrCreate();

        List<Row> inMemory = new ArrayList<>();

        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);
        dataset.show();

        /**
         * Group together all the different log levels together (Aggregation based on level)
         * select level, datetime from logging_table GROUP BY level -> SQL Syntax
         * Result -> WARN [2016-12-31 04:19:32, 2016-12-31 03:21:21]
         *  - The below query fails because when you do GROUP BY, you will get level, LIST<String datetime> and this can return millions of files
         *  - Best approach is to combine GROUP BY with an aggregation function on the column that is being aggregated.
         * */

        dataset.createOrReplaceTempView("logging_table");
        /**
         * Below query commented out because it will give an error
         * Good for understanding though
         * */
//        Dataset<Row> groupedDataset = spark.sql("Select level, datetime from logging_table GROUP BY level");
//        groupedDataset.show();

        /**
         * Aggregating on the datetime column by using count
         * Lots of aggregation available in spark -> More than databases
         * https://spark.apache.org/docs/latest/api/sql/index.html
         * */
        Dataset<Row> result = spark.sql("Select level, count(datetime) from logging_table GROUP BY level order by level");
        result.show();

        /**
         * Inorder to run the commented query, Go through the below code
         * We use collect_list method. This is same as groupBy in SparkCore -> Dangerous because it affects the performance
         * */
        Dataset<Row> groupedDataset = spark.sql("Select level, collect_list(datetime) from logging_table GROUP BY level");
        groupedDataset.show();

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
