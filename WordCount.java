import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String str = "Akshay is a good boy. Akshay is nice";
        List<String> data = Arrays.asList(str.split(" "));
        JavaRDD<String> javaRdd = sc.parallelize(data);
        JavaPairRDD<String, Integer> wordCount = javaRdd.mapToPair(a -> new Tuple2<>(a, 1)).reduceByKey((a, b) -> a + b);
         wordCount.foreach((k)-> {
            System.out.println("Word :" +  k._1 + " Count : " + k._2);
        });
    }
}
