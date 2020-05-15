package SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import java.util.Scanner;

public class _09_DataFrames {
    /**
     * DataFrames API : Set of Java APIs for Spark SQL
     *                  Alternatives to traditional SQL.
     * SparkSQL has 3 sets of API
     *  - SparkSQL : Processing using traditional SQL
     *  - DataFrames API : Java APIs instead of traditional SQL (Dataset<Rows>)
     *  - Datasets API : Identical to DataFrames API but it creates Dataset<CUSTOM_OBJECTS>
     */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]")
                .config("spark.sql.warehouse", "file:///C:/Akshay Github/tmp")
                .getOrCreate();

        /**
         * We will use the example from _08_Ordering.java and convert the SparkSQL to Spark DataFrame
         * */

        Dataset<Row> dataset = spark.read().option("header", true).csv("src\\main\\resources\\biglog.txt.txt");
        dataset.createOrReplaceTempView("logging_big_table");
        Dataset<Row> result = spark.sql("SELECT level, date_format(datetime,'MMMM') AS month, count(1) FROM logging_big_table GROUP BY month, level ORDER BY cast(first(date_format(datetime, 'M')) AS int) , level");
        result.show(60, false);

//        dataset.selectExpr("level", "date_format(datetime,'MMMM') as month"); We can use this technique but this is not purely Java sp we will skip it for now
        dataset = dataset.select(col("level"),
                                 date_format(col("datetime"),"MMMM").alias("month"),
                                 date_format(col("datetime"),"M").alias("month_num").cast(DataTypes.IntegerType));

        /**
         * Now we want to group by level and month.
         * dataset.groupBy(col("level"), col("month")) return a RelationalGroupedDatase<Row> and not Dataset<Row>.
         * To fix, we have to perform aggregation on columns which are not grouped.
         * */

        dataset = dataset.groupBy(col("level"), col("month"), col("month_num")).count();
        dataset = dataset.orderBy(col("month_num"), col("level"));
        dataset.show(60, false);

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
