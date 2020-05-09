package SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

public class _08_Ordering {
    /**
     * Ordering
     */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]")
                .config("spark.sql.warehouse", "file:///C:/Akshay Github/tmp")
                .getOrCreate();

        /**
         * The above dataset is difficult to digest because it is not ordered.
         * I have created another column 'num_month' which contains the numerical values of month so that we can order by that
         *  - The query fails  because whenever you perform grouping, you have to aggregate the other columns
         *  Getting rid of num_month column since it is meaningless
         *        - Java API method : result.drop() -> create a new dataset
         *        - SQL way : instead of creating a variable num_month, simply use the entire phase in ORDER BY
         * */

        Dataset<Row> dataset = spark.read().option("header", true).csv("src\\main\\resources\\biglog.txt.txt");
        dataset.createOrReplaceTempView("logging_big_table");
        Dataset<Row> result = spark.sql("SELECT level, date_format(datetime,'MMMM') AS month, count(1), cast(first(date_format(datetime, 'M')) AS int) as num_month FROM logging_big_table GROUP BY month, level ORDER BY num_month, level");
        result = result.drop("num_month");
        result.show(60, false); // truncate : is for columns and not rows

        result = spark.sql("SELECT level, date_format(datetime,'MMMM') AS month, count(1) FROM logging_big_table GROUP BY month, level ORDER BY cast(first(date_format(datetime, 'M')) AS int) , level");
        result.show(60, false);

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
