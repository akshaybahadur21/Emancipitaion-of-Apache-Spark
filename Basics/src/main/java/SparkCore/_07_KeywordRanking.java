package SparkCore;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class _07_KeywordRanking {
    /**
     *
     * Keyword Ranking
     * Figure out most important Keywords
     * important links to follow
     *          - https://spark.apache.org/docs/latest/rdd-programming-guide.html
     *          - https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations
     *          - https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions
     *
     * Tranform and perform actions on RDD
     * */

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("KeywordRanking").setMaster("local[*]"); // local[*] means to run spark locally and
                                                                                       // and assign all the threads for parallel execution
        JavaSparkContext sc = new JavaSparkContext(conf);

//        JavaRDD<String> fromDiskRDD = sc.textFile("src/main/resources/subtitles/input.txt"); // this can be from hdfs or s3 (big data)
        JavaRDD<String> fromDiskRDD = sc.textFile("src/main/resources/subtitles/input-spring.txt"); // this can be from hdfs or s3 (big data)
        JavaRDD<String> boringRdd = sc.textFile("src/main/resources/subtitles/boringwords.txt");
        List<String> boringList = boringRdd.map(a -> a.toLowerCase())
                .collect();

        System.out.println(" *********** Keyword Ranking *****************");
        System.out.println(" *********** Transformed Data *****************");
        JavaRDD<String> flatMapRDD = fromDiskRDD.flatMap(a -> Arrays.asList(a.split(" ")).iterator());
        JavaRDD<String> transformedRDD = flatMapRDD.filter(a -> a.matches("^[a-zA-Z]+$"))
                .filter(a -> a.length() > 1)
                .map(a -> a.toLowerCase())
                .filter(a -> !boringList.contains(a));
        transformedRDD.take(50).forEach(a-> System.out.println(a));

        System.out.println(" *********** Counting Data *****************");
        JavaPairRDD<String, Long> countRDD = transformedRDD.mapToPair(a -> new Tuple2<>(a, 1L))
                .reduceByKey((a, b) -> a+b); //can also do count by key
        countRDD.take(50).forEach(a -> System.out.println(a._1 +" -->"+ a._2));

        JavaPairRDD <Long,String> switchedRDD = countRDD.mapToPair(a -> new Tuple2(a._2,a._1));  // switching keys so that we could sort using the keys in the next step

        JavaPairRDD<Long, String> sortedRDD = switchedRDD.sortByKey(false);
        System.out.println(" *********** Sorted Data *****************");
        sortedRDD.take(50).forEach(a -> System.out.println(a._2 + ", " +a._1));

        sc.close();
    }
}
