package SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class _15_HashAggregations {
    /**
     * Hash Aggregations : In the last module, we saw SQL performing 4 times less than equivalent Java API.
     * However, in reality, there is no appreciable difference between performance.
     *
     */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]")
                .config("spark.sql.warehouse", "file:///C:/Akshay Github/tmp")
                .getOrCreate();
//        spark.conf().set("spark.sql.shuffle.partitions", 12);

        Dataset<Row> dataset = spark.read().option("header", true).csv("src\\main\\resources\\biglog.txt.txt");
        dataset.createOrReplaceTempView("logging_table");

        Dataset<Row> results = spark.sql
                ("select level, date_format(datetime,'MMMM') as month, count(1) as total " +
                        "from logging_table group by level, month order by cast(first(date_format(datetime,'M')) as int), level");
        results.show();
        /**
         * For investigating, we can call explain(). It will show the execution plan.
         * We read the plan from bottom - up.
         * If the step has an Asterisk(*) means that it is part of whole stage codegen
         *
         * Let's debug the execution plan
         * == Physical Plan ==
         *  select        (3) Project [level#10, month#14, total#15L]
         *  orderby     - *(3) Sort [aggOrder#45 ASC NULLS FIRST, level#10 ASC NULLS FIRST], true, 0
         *                 +- Exchange rangepartitioning(aggOrder#45 ASC NULLS FIRST, level#10 ASC NULLS FIRST, 200)
         *                    +- SortAggregate(key=[level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(Asia/Calcutta))#47], functions=[count(1), first(date_format(cast(datetime#11 as timestamp), M, Some(Asia/Calcutta)), false)])
         *                       +- *(2) Sort [level#10 ASC NULLS FIRST, date_format(cast(datetime#11 as timestamp), MMMM, Some(Asia/Calcutta))#47 ASC NULLS FIRST], false, 0
         *                          +- Exchange hashpartitioning(level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(Asia/Calcutta))#47, 200)
         *  grouping                   +- SortAggregate(key=[level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(Asia/Calcutta)) AS date_format(cast(datetime#11 as timestamp), MMMM, Some(Asia/Calcutta))#47], functions=[partial_count(1), partial_first(date_format(cast(datetime#11 as timestamp), M, Some(Asia/Calcutta)), false)])
         *  codegen                       +- *(1) Sort [level#10 ASC NULLS FIRST, date_format(cast(datetime#11 as timestamp), MMMM, Some(Asia/Calcutta)) AS date_format(cast(datetime#11 as timestamp), MMMM, Some(Asia/Calcutta))#47 ASC NULLS FIRST], false, 0
         *  codegen                          +- *(1) FileScan csv [level#10,datetime#11] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/Akshay GitHub/Emancipitaion-of-Apache-Spark/Basics/src/main/resources/..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<level:string,datetime:string>
         *
         * */
        results.explain();

        dataset = dataset.select(col("level"),
                date_format(col("datetime"),"MMMM").alias("month"),
                date_format(col("datetime"),"M").alias("month_num").cast(DataTypes.IntegerType));

        dataset = dataset.groupBy(col("level"), col("month"), col("month_num")).count().as("Total").orderBy("month_num");
        dataset = dataset.drop("month_num");
        dataset.show();

        /**
         * Let's compare execution plan for identical Java API
         *
         *      == Physical Plan ==
         *      *(3) Project [level#10, month#54, count#64L]
         *      +- *(3) Sort [month_num#56 ASC NULLS FIRST], true, 0
         *         +- Exchange rangepartitioning(month_num#56 ASC NULLS FIRST, 200)
         *            +- *(2) HashAggregate(keys=[level#10, month#54, month_num#56], functions=[count(1)])
         *               +- Exchange hashpartitioning(level#10, month#54, month_num#56, 200)
         *                  +- *(1) HashAggregate(keys=[level#10, month#54, month_num#56], functions=[partial_count(1)])
         *                     +- *(1) Project [level#10, date_format(cast(datetime#11 as timestamp), MMMM, Some(Asia/Calcutta)) AS month#54, cast(date_format(cast(datetime#11 as timestamp), M, Some(Asia/Calcutta)) as int) AS month_num#56]
         *                        +- *(1) FileScan csv [level#10,datetime#11] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/Akshay GitHub/Emancipitaion-of-Apache-Spark/Basics/src/main/resources/..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<level:string,datetime:string>
         *
         * Straight away, we notice some differences.
         * 1. In SQL we see Sort and SortAggregate in middle stages, whereas in Java API, we have Hash Aggregation
         *
         * */
        dataset.explain();

        /**
         * Spark SQL can use 2 algorithms for grouping
         *      - Sort Aggregate(Spark SQL)
         *          Sort Aggregation sorts the rows and groups them together
         *          Performance : O(nlogn) and O(1). This sort does not work with large amount of data
         *
         *      - Hash Aggregate(Spark SQL Java API)
         *          Spark Creates a table of key value pairs(HashMap). Spark SQL creates a hash key from the grouping columns (efficient)
         *          The value will be rest of the columns
         *          In case of collision, spark will update the 'value'
         *          Performance : O(n) and O(1)
         * Side Note : Search for HashAggregate vs GroupAggregate
         * */

        /**
         * Strategy
         * SparkSQL tries to use Hash Aggregation as much as possible.
         * Hash Aggregation can only be done if data in the 'value' is mutable(mutable in spark)
         * Spark implements HashMap in native memory and not as Java Objects(Java Heap). So the spark hashmap is written in memory directly
         * In order to do that, you have to use sun.misc.Unsafe operations, where you can leave Java Heap and directly work with memory. (Removed from Java 9)
         * Spark updates the value in the HashAggregate map and it can only be done if the value are mutable(only a small number of datatypes are mutable)
         * To find out about all the mutable datatypes -> https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-UnsafeRow.html ; https://github.com/apache/spark/blob/master/sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/UnsafeRow.java
         *
         * To sum up, we want to make sure that all the data that's not a part of grouping must be in UnsafeRow class (mutable type)
         * Spark can mutate the above mentioned datatypes because each of those types has a fixed range (32 bits for integer). String is variable and hence spark can't mutate it
         * */

        results = spark.sql
                ("select level, date_format(datetime,'MMMM') as month, count(1) as total, first(cast(date_format(datetime,'M') as int)) as monthnum " +
                        "from logging_table group by level, month order by monthnum, level");
        results.drop("monthnum").show();
        results.explain();

        /**
         * Another thing that you could do to ensure Hash Aggregation is to include non mutable datatypes in the grouping key. So that value contains only mutable values.
         * */

        results = spark.sql
                ("select level, date_format(datetime,'MMMM') as month, count(1) as total,date_format(datetime,'M') as monthnum " +
                        "from logging_table group by level, month, monthnum order by monthnum, level");
        results.drop("monthnum").show();
        results.explain();

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
