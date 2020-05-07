package SparkCore;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class _4_PairRDD {
    /**
     *
     * Creating a PairRDD
     * Example to check the levels of Logging in a log file.
     * Much like in map in java however the Key can be common for multiple rows. (Google Guava has this concept of multiple same keys)
     * Why do we choose PairRDDs over Tuple -> PairRDDs give us rich functionalities over Tuple
     *
     * */
    public static void main(String[] args) {
        List<String> data = new ArrayList<>();
        data.add("WARN : Tuesday 4 September 0405");
        data.add("ERROR : Tuesday 4 September 0408");
        data.add("FATAL : Wednesday 5 September 1632");
        data.add("ERROR : Friday 7 September 1854");
        data.add("WARN : Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Mapping").setMaster("local[*]"); // local[*] means to run spark locally and
                                                                                      // and assign all the threads for parallel execution
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> javaRDD = sc.parallelize(data);
        System.out.println(" *********** MAP TO PAIR *****************");
        JavaPairRDD<String, String> javaPairRDD = javaRDD.mapToPair(a -> new Tuple2<>(a.split(":")[0], a.split(":")[1]));
        javaPairRDD.foreach(a -> System.out.println(a._1 + ", " + a._2));

        //count by key
        System.out.println(" *********** COUNT BY KEY *****************");
        Map<String, Long> temp = javaPairRDD.countByKey();
        temp.forEach((k,v) -> System.out.println(k + " ->" + v));

        //Reduce by Key (only available in PairRDD)
        System.out.println(" *********** Reduce BY KEY *****************");
        JavaPairRDD<String, Integer> tempRdd = javaRDD.mapToPair(a -> new Tuple2<>(a.split(":")[0], 1));
        JavaPairRDD<String, Integer> reduceByKeyAns = tempRdd.reduceByKey((a, b) -> a + b);
        reduceByKeyAns.foreach(a-> System.out.println(a._1 +", " + a._2));

        //Using fluent APIs
        System.out.println(" *********** FLUENT API *****************");
        sc.parallelize(data)
                .mapToPair(a -> new Tuple2<>(a.split(":")[0], 1))
                .reduceByKey((a,b) -> a+b)
                .foreach(a -> System.out.println(a._1 +", " + a._2));

        //Can we group by Key as well (Performance is low in groupByKey)
        System.out.println(" *********** GROUP BY KEY *****************");
        JavaPairRDD<String, Iterable<String>> gropByKeyRdd = javaPairRDD.groupByKey();
        gropByKeyRdd.foreach(a -> System.out.println(a._1 +", " + Iterables.size(a._2)));

        sc.close(); //Necessary to close the spark context. Good practise it to close it in the
                    // finally block and put everything else in try block
    }

}
