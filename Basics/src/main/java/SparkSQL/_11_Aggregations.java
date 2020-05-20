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

import static org.apache.spark.sql.functions.*;

public class _11_Aggregations {
    /**
     * More Aggregations
     *
     */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]")
                .config("spark.sql.warehouse", "file:///C:/Akshay Github/tmp")
                .getOrCreate();

        /**
         *  Dataset<Row> dataset = spark.read().option("header", true).option("inferSchema",true).csv("src\\main\\resources\\exams\\students.csv");
         * .option("inferSchema",true) -> will automatically cast all columns based on their datatype(expensive)
         * use agg() method for aggregation on columns
         * */
        Dataset<Row> dataset = spark.read().option("header", true).csv("src\\main\\resources\\exams\\students.csv");
        dataset.createOrReplaceTempView("student_table");

        //Get maximum score from all subjects
        dataset.groupBy("subject").agg(max(col("score").cast(DataTypes.IntegerType).alias("Max Score"))).show();

        //Get maximum, minimum, average score from all subjects
        dataset.groupBy("subject").agg(max(col("score").cast(DataTypes.IntegerType).alias("Max Score")),
                                            min(col("score").cast(DataTypes.IntegerType).alias("Min Score")),
                                            mean(col("score").cast(DataTypes.IntegerType).alias("AVG Score"))).show();


        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
