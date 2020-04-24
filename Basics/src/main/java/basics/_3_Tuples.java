package basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple5;

import java.util.ArrayList;
import java.util.List;

public class _3_Tuples {
    /**
     *
     * Creating a Tuple
     * We want to create a map of Map <number, sqrt of number>
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

        JavaRDD<TransformToMap> sqrtRddUsingInnerClass = javaRDD.map(a -> new TransformToMap(a)); //This is not preferred (Use Tuples)
        System.out.println("JavaRDD<TransformToMap>");
        sqrtRddUsingInnerClass.foreach(a -> System.out.println(a.number  + ", " + a.sqrtNumber));
        System.out.println();
        //  **********************            OR            ************************    //

        JavaRDD<Tuple2<Integer,Double>> tempRdd = javaRDD.map(a -> new Tuple2<>(a, Math.sqrt(a)));
        System.out.println("Printing JavaRDD<Tuple2<Integer,Double>>");
        tempRdd.foreach( a -> System.out.println(a._1  + ", " + a._2));
        System.out.println();
        /**
         *
         * Working with PairRDDs <key, value>
         * Help us grouping by key
         *
         * */
        JavaPairRDD<Integer, Double> sqrtRdd = javaRDD.mapToPair(a -> new Tuple2<>(a, Math.sqrt(a))); //scala code would be rdd = (1,2)
        System.out.println("Printing JavaPairRDD<Integer, Double>");
        sqrtRdd.foreach(a -> System.out.println(a._1  + ", " + a._2));
        System.out.println();

        sc.close(); //Necessary to close the spark context. Good practise it to close it in the
                    // finally block and put everything else in try block
    }

    private static class TransformToMap {
        private int number;
        private double sqrtNumber;
        public TransformToMap(Integer a) {
            this.number = a;
            this.sqrtNumber = Math.sqrt(a);
        }
    }
}
