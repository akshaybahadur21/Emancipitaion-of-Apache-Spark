package SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

import static org.apache.spark.sql.functions.col;

public class _03_TemporaryView {
    /**
     *
     * Spark Temporary Views for SQL
     * You can't execute a full SQL statement, you only pass value after the WHERE clause.
     * Because of this, we miss out on functionalities like COUNT, AVERAGE etc
     *
     * */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]")
                                        .config("spark.sql.warehouse","file:///C:/Akshay Github/tmp")
                                        .getOrCreate();
        Dataset<Row> dataset = spark.read().option("header", true).csv("src\\main\\resources\\exams\\students.csv");
        Dataset<Row> moderArtDataset = dataset.filter(" subject = 'Modern Art'");
        moderArtDataset.show();

        /**
         * For traditional SQL queries (full sql syntax), we can use SparkViews
         * Necessity : We can create a view "Students"
         * We can use Students in a later sql query
         * We have to use spark.sql() to query using SQL queries from the table "Students"
         * */
        dataset.createOrReplaceTempView("Students");
        Dataset<Row> studentsViewDataset = spark.sql("Select student_id, score,grade from Students where subject = 'French'");
        studentsViewDataset.show();

        spark.sql("SELECT max(score) from Students where subject = 'French'").show();
        spark.sql("SELECT avg(score) from Students where subject = 'French'").show();
        spark.sql("SELECT min(score) from Students where subject = 'French'").show();

        spark.sql("SELECT distinct year from Students order by year").show();

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
