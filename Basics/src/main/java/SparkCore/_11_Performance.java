package SparkCore;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class _11_Performance {
    /**
     *
     * Performance Aspects
     *   1. Transformation and Action
     *   2. The DAG (Execution Plan) / WebUI
     *   3. Narrow and Wide Transformations -> Shuffle and Stages
     *   4. Partitions and Key Skews
     *   5. Avoiding GroupByKey
     *   6. Caching and Persistence
     *
     * */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Performance").setMaster("local[*]"); // local[*] means to run spark locally and
        // and assign all the threads for parallel execution
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         *
         * 1. Transformation and Action
         *
         * The below example is taken from _7_KeywordRanking.java
         * You might feel the Spark is creating RDDs for every step and that's NOT TRUE.
         * Spark only processes or does calculation when an ACTION is done -> when filteredLowerCaseRDD.take(10); is performed
         * All the other lines (TRANSFORMATION) only build the execution plan (LAZILY EXECUTED)
         * We don't build a new RDD at every TRANSFORM step, rather we just add a step to the execution plan.
         * Even reading in data will also take place once Spark has to perform ACTION
         *
         * */
        JavaRDD<String> boringRdd = sc.textFile("src/main/resources/subtitles/boringwords.txt");
        List<String> boringList = boringRdd.map(a -> a.toLowerCase()).collect();
        JavaRDD<String> fromDiskRDD = sc.textFile("src/main/resources/subtitles/input-spring.txt"); // this can be from hdfs or s3 (big data)
        JavaRDD<String> flatMapRDD = fromDiskRDD.flatMap(a -> Arrays.asList(a.split(" ")).iterator());
        JavaRDD<String> transformedRDD = flatMapRDD.filter(a -> a.matches("^[a-zA-Z]+$"));
        JavaRDD<String> filteredRDD = transformedRDD.filter(a -> a.length() > 1);
        JavaRDD<String> filteredLowerCaseRDD = filteredRDD.map(a -> a.toLowerCase())
                .filter(a -> !boringList.contains(a));
        filteredLowerCaseRDD.take(10);


        /**
         *
         * 2. The DAG (Execution Plan) / WebUI
         *
         * When Spark is running, web server will be started on localhost:4040
         * So I will take all the code from above and when spark context is about to close, I will simply put a scanner in place.
         * Then you can log onto spark UI
         *
         * */



        /**
         *
         * 3. Narrow(STAGE) and Wide(SHUFFLING) Transformations -> Shuffle and Stage
         *
         * Spark tells the worker nodes to load data into memory in fragments (PARTITIONS)
         * We see multiple partitions on each of the worker node.
         * How does spark determine partitioning - Depends on the input file (Spark chunks the data 64MB each) and each line
         *                                          could then be sent to the worker nodes
         * let's say if you have a transformation in place
         * eg : filter(a -> a.startWith("WARN")) -> This will be distributed amongst the partitions in each of the node where it becomes a task.
         * TASK - A block of code executing against a partition.
         * Since this type of transformation can be applied individually to each partition without moving data around -> NARROW TRANSFORMATIONS
         *
         * eg : rdd.groupByKey() -> creates a pairRDD based on the Keys. If I want to get all the log files with the level
         * 'WARN', spark will move the data around different partitions across multiple nodes(serialize and send across network)
         * Since this type of transformation involves moving the data around -> WIDE TRANSFORMATIONS or SHUFFLING
         *
         * */

        /**
         *
         * 4. Shuffles
         *
         * You should avoid shuffling but sometimes it is necessary.
         * You need to make sure that you can optimize RDD before shuffling (less data to shuffle)
         * STAGE - A series of transformation that doesn't need a shuffle
         * After shuffling, you might get data skewness. Let's say you groupByKey() and there are 3 keys and 11 partitions
         * The resulting RDD will have 3 partition with all the data and 8 with no data (KEY SKEWNESS)
         * Your cluster is under-utilized.
         *
         * */

        /**
         *
         * 5. Key Skew
         *
         * - Make sure that wide transformations(Joins) happen later after ou have completed all the previous steps.
         * - SALTING KEYS : You artificially add some 'salt number' to end of the keys so that they are distributed across the cluster
         *                  <WARN1 , [a,b]> ; <WARN2, [g.h.d]>
         *
         * */

        /**
         *
         * 6. Avoid groupByKey()
         *
         * After groupByKey, the data is shuffled and causes the keys to be skewed and majority of the data o be stored in 1 node
         * Alternatively, we can use REDUCE -> reduceByKey()
         * reduceByKey() also shuffles the data, however it is less expensive that groupByKey() since reduceByKey() has 2 phases
         * 1st phase can be done individually on each partition without shuffling (subtotal on each partition)
         * 2nd phase we do the shuffling but since we have already done MAP-SIDE REDUCE, the data shuffled is much less.
         * Then we apply the reduce again to get final total.
         *
         * */

        /**
         *
         * 7. Caching and Persistence
         * So for each ACTION, the execution plan is executed all over again from the start.
         * There is some sort of caching involved though. Since results of Shuffles (Wide Transformation) are stored in disk, this information is reused in subsequent ACTIONS.
         * - Cache : Cache an RDD for future reference (checkpointing) -> resultRDD = resultRDD.cache() [Need space in RAM]
         * - Persist : Persist an RDD in RAM or Disk or both -> resultRDD = resultRDD.persist(StorageLevel.MEMORY_AND_DISK()
         *
         * */
        filteredLowerCaseRDD = filteredLowerCaseRDD.cache();
        filteredLowerCaseRDD = filteredLowerCaseRDD.persist(StorageLevel.MEMORY_AND_DISK());

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        sc.close();
    }
}
