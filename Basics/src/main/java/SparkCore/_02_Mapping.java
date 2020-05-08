package SparkCore;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class _02_Mapping {
    /**
     *
     * Creating a map
     * RDD is immutable so a new mapRdd is created.
     * Datatypes can be different of i/p and o/p in Spark Maps
     *
     * */

    public static void main(String[] args) {
        List<Integer> data = new ArrayList<>();
        data.add(20);
        data.add(25);
        data.add(99);
        data.add(525);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Mapping").setMaster("local[*]"); // local[*] means to run spark locally and
                                                                                         // and assign all the threads for parallel execution
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> javaRDD = sc.parallelize(data);
        JavaRDD<Double> sqrtRdd = javaRDD.map(a -> Math.sqrt(a));
        sqrtRdd.foreach(a -> System.out.println(a)); // will crash if I do sqrtRdd.foreach(System.out :: println);
                                                     // since PrintStream is not serializable
                                                     // Add a .collect() for that sqrtRdd.collect().forEach(System.out :: println)

        //Number of elements in sqrtRdd

        long numSqrtRdd = sqrtRdd.count();
        System.out.println(numSqrtRdd);

        // Counting just using map and reduce

        JavaRDD<Integer> singleIntegerRdd = sqrtRdd.map(a -> 1);
        Integer countUsingMapAndReduce = singleIntegerRdd.reduce((a, b) -> a + b);
        System.out.println(countUsingMapAndReduce.intValue());

        sc.close(); //Necessary to close the spark context. Good practise it to close it in the
                    // finally block and put everything else in try block
    }
}
