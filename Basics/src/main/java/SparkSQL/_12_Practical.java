package SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class _12_Practical {
    /**
     * Practical Question
     * Aim : Build a pivot table with subject on left hand side , year across the top
     *    - Average score
     *    - Standard deviation
     *
     */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]")
                .config("spark.sql.warehouse", "file:///C:/Akshay Github/tmp")
                .getOrCreate();


        Dataset<Row> dataset = spark.read().option("header", true).csv("src\\main\\resources\\exams\\students.csv");
        dataset.createOrReplaceTempView("student_table");
        dataset.groupBy("subject").pivot("year").agg( round(avg(col("score").cast(DataTypes.DoubleType)), 2).alias("AVG Score"),
                                                        round(stddev(col("score").cast(DataTypes.DoubleType)),2).alias("STD Dev")).show();


        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
