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

public class _07_MultipleGroupings {
    /**
     *
     * Multiple Grouping
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

        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> yearDataset = spark.sql("SELECT level, date_format(datetime,'yyyy') FROM logging_table");
        yearDataset.show();

        Dataset<Row> monthDataset = spark.sql("SELECT level, date_format(datetime,'MMMM') as month FROM logging_table");
        monthDataset.show();

        /**
         * So basically we need to find out which month had which log level.
         * So we can do a group by month, level
         * Whenever we do grouping, we must do AGGREGATION but aggregation can only be done on a column which is not in GROUP BY.
         * So we need to add a dummy column to the dataset
         * */
        monthDataset.createOrReplaceTempView("logging_table"); //overwriting
        Dataset<Row> result = spark.sql("SELECT level, month, count(1) as total FROM logging_table GROUP BY level, month");
        result.show();

        /**
         * Reading in a big data file amd performing same steps as before
         * */

        dataset = spark.read().option("header",true).csv("src\\main\\resources\\biglog.txt.txt");
        dataset.createOrReplaceTempView("logging_big_table");
        spark.sql("SELECT level, date_format(datetime,'MMMM') as month FROM logging_big_table").createOrReplaceTempView("logging_big_table");
        result = spark.sql("SELECT level, month, count(1) as total FROM logging_big_table GROUP BY level, month");
        result.show(60, false); // truncate : is for columns and not rows

        result.createOrReplaceTempView("result_table");
        Dataset<Row> totalDataset = spark.sql("SELECT SUM(total) FROM result_table");
        totalDataset.show(); // this should be 1 million

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
