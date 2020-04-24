package basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class _9_Joins {
    /**
     *
     * Joins - We can perform joins on RDDs similar to those on SQL.
     *
     * */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Joins").setMaster("local[*]"); // local[*] means to run spark locally and
        // and assign all the threads for parallel execution
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         *
         * We can perform different joins most commen being the inner join (or just join in spark API)
         * 1. Inner Join
         *
         * */
        List<Tuple2<Integer, Integer>>  visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4,8));
        visitsRaw.add(new Tuple2<>(6,4));
        visitsRaw.add(new Tuple2<>(10,9));

        List<Tuple2<Integer, String>>  usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Mary"));
        usersRaw.add(new Tuple2<>(6, "Akshay"));

        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRDD = visits.join(users);
        System.out.println(" *********** joinedRDD *****************");
        joinedRDD.collect().forEach(System.out::println);

        System.out.println(" *********** Reverse joinedRDD *****************");
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinedReversRDD = users.join(visits);
        joinedReversRDD.collect().forEach(System.out::println);

        /**
         *
         * 2. Left Outer Join
         *
         * */
        System.out.println(" *********** Left Outer Join *****************");
        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftOuterJoinRDD = visits.leftOuterJoin(users);
        leftOuterJoinRDD.collect().forEach(System.out::println);
        leftOuterJoinRDD.collect().forEach(a-> System.out.println(a._2._2.orElse("null").toUpperCase()));

        /**
         *
         * 2. Right Outer Join
         *
         * */

        System.out.println(" *********** Right Outer Join *****************");
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoinRDD = visits.rightOuterJoin(users);
        rightOuterJoinRDD.collect().forEach(System.out::println);
        rightOuterJoinRDD.collect().forEach(a-> System.out.println("user " + a._2._2 + " has visits " + a._2._1.orElse(0)));

        /**
         *
         * 2. Full Outer Join, Cartestian (Cross Join)
         *
         * */

        System.out.println(" *********** Full Outer Join *****************");
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullOuterJoinRDD = visits.fullOuterJoin(users);
        fullOuterJoinRDD.collect().forEach(System.out::println);
        fullOuterJoinRDD.collect().forEach(a-> System.out.println("user " + a._2._2.orElse("null").toUpperCase() + " has visits " + a._2._1.orElse(0)));

        System.out.println(" *********** Cartesian *****************");
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesianRDD = visits.cartesian(users);
        cartesianRDD.collect().forEach(System.out::println);
        sc.close();
    }
}
