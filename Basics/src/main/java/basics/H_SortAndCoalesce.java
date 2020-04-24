package basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class H_SortAndCoalesce {
    /**
     *
     * Sort and Coalesce
     *
     * */

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("KeywordRanking").setMaster("local[*]"); // local[*] means to run spark locally and
        // and assign all the threads for parallel execution
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> fromDiskRDD = sc.textFile("src/main/resources/subtitles/input.txt"); // this can be from hdfs or s3 (big data)
        JavaRDD<String> boringRdd = sc.textFile("src/main/resources/subtitles/boringwords.txt");
        List<String> boringList = boringRdd.map(a -> a.toLowerCase())
                .collect();

        System.out.println(" *********** Keyword Ranking *****************");
        JavaRDD<String> flatMapRDD = fromDiskRDD.flatMap(a -> Arrays.asList(a.split(" ")).iterator());
        JavaRDD<String> transformedRDD = flatMapRDD.filter(a -> a.matches("^[a-zA-Z]+$"))
                .filter(a -> a.length() > 1)
                .map(a -> a.toLowerCase())
                .filter(a -> !boringList.contains(a));

        JavaPairRDD<String, Long> countRDD = transformedRDD.mapToPair(a -> new Tuple2<>(a, 1L))
                .reduceByKey((a, b) -> a+b); //can also do count by key
        JavaPairRDD <Long,String> switchedRDD = countRDD.mapToPair(a -> new Tuple2(a._2,a._1));  // switching keys so that we could sort using the keys in the next step

        JavaPairRDD<Long, String> sortedRDD = switchedRDD.sortByKey(false); // sorting by Key in Descending Order
        System.out.println("No of Partitions : " + sortedRDD.getNumPartitions());
        System.out.println(" *********** Sorted Data *****************");
        //Instead of take, we do a foreach on the RDD
        sortedRDD.foreach(a -> System.out.println(a._1 +" -->"+ a._2));

        //Once we do this, the order looks different
        /**
         * Why does this happen?
         * Wrong explanation - The data will be divided into different partitions or different nodes
         *                     The returned data is sorted on a partition basis.
         *                     Remember, this is the wrong explanation
         *                     Although, if we go by this approach, we want the data to be in a single partition before sort. (Ue COALESCE)
         *
         * */

        System.out.println(" *********** AFTER COALESCE *****************");
        sortedRDD.coalesce(1).foreach(a -> System.out.println(a._1 +" -->"+ a._2));

        /**
         *
         * Coalesce is wrong since it combines the entire data into 1 partition or 1 node (against the concept of Big Data)
         * Might be okay if it is the last step since you must have already transformed the data and done everything else
         * Correct Explanation - The foreach runs in parallel on the partitions in parallel and therefore the threads of for each
         *                       compete with each other to get the execution time giving us a mixed output.
         *                      A good approach is to do take(n) to take the sorted values
         *
         * */

        /**
         *
         * COALESCE AND COLLECT
         * Both work similar. Coalesce gets the data into n partitions (maybe 1 on a single node for repartitioning)
         * Collect will collect the entire RDD into JAVA VM RAM for printing
         *
         * */

        sc.close();
    }
}
