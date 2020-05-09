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

public class _06_DateFormatting {
    /**
     * Date Formatting
     * Picking up from the previous example, we want to aggregate the warnings 'FATAL', 'WARN', 'INFO' for each month
     * To figure out it there is a problem for certain months
     */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]")
                .config("spark.sql.warehouse", "file:///C:/Akshay Github/tmp")
                .getOrCreate();

        List<Row> inMemory = new ArrayList<>();

        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL", "2015-4-21 19:23:20"));

        StructField[] fields = new StructField[]{
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);
        dataset.show();

        dataset.createOrReplaceTempView("logging_table");
        /**
         * Use date_format() from API docs to convert string to date
         * date_format("String", "fmt")
         * - String : Your date in String
         * - fmt : Date format
         * REFER to SimpleDateFormat Class in java docs
         * */
        Dataset<Row> yearDataset = spark.sql("SELECT level, date_format(datetime,'yyyy') FROM logging_table");
        yearDataset.show();

        Dataset<Row> monthDataset = spark.sql("SELECT level, date_format(datetime,'MMMM') as month FROM logging_table");
        monthDataset.show();

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
