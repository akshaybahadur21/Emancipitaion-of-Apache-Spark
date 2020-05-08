package SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class _02_Datasets {
    /**
     *
     * Exploring Datasets in Spark SQL
     * 1. Basics
     * - Filters in SQL
     *     2. Filters using Expressions
     *     3. Filters using Lambdas
     *     4. Filter using Columns
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
        dataset.show();
        Row firstRow = dataset.first();
        /***
         *
         * 1. Basics
         *
         * Below are the columns for the dataset.
         * +----------+--------------+----------+----+-------+-----+-----+
         * |student_id|exam_center_id|subject|year|quarter|score|grade|
         * +----------+--------------+----------+----+-------+-----+-----+
         * firstRow.get(2) -> getting 'subject' from a row
         *
         */

        String subject = String.valueOf(firstRow.get(2));
        System.out.println(subject);

        /**
         * Since we have the headers from our source, we can use them to get a specific column
         * We can use a getAs() method which will also perform automatic conversion
         * firstRow.getAs("subject")
         * */
        subject = firstRow.getAs("subject");
        System.out.println(subject);

        int year = Integer.valueOf(firstRow.getAs("year"));
        System.out.println(year);

        /**
         *
         * 2. Filters using Expressions
         * Suppose you want to filter out dataset based on some expression.
         * Similar ro what we write in WHERE clause in SQL
         * Under the hood, everything is working in RDDs
         *
         * */

        Dataset<Row> modernArtDataset = dataset.filter(" subject = 'Modern Art' and year >= 2007 ");
        modernArtDataset.show();

        /**
         *
         * 3. Filters using Lambdas
         * Each element in lambda is of type Row
         *
         * */

        modernArtDataset = dataset.filter(row -> row.getAs("subject").equals("Modern Art")
                                                && Integer.valueOf(row.getAs("year")) >= 2007);
        modernArtDataset.show();

        /**
         *
         * 4. Filters using Columns
         * Programmatic approach : We don't need to concatenate the SQL query or lengthy lambda functions
         *
         * */

        Column subjectColumn = dataset.col("subject");
        Column yearColumn = dataset.col("year");
        modernArtDataset = dataset.filter(subjectColumn.equalTo("Modern Art")
                                                        .and(yearColumn.geq(2007)));
        modernArtDataset.show();

        /**
         * We can go for a slight improvement by importing
         * import static org.apache.spark.sql.functions.*; which will get col() function to return Columns based on column name
         * Makers of API want it to be a domain specific language
         * domain specific language = Code is almost equal to SQL language
         * */

        modernArtDataset = dataset.filter(col("subject").equalTo("Modern Art")
                            .and(col("year").geq(2007)));
        modernArtDataset.show();

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
