package basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class E_FlatMaps {
    /**
     *
     * Creating a FlatMap
     * For one key, you can have multiple outputs or no outputs (flat collection)
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
        SparkConf conf = new SparkConf().setAppName("FlatMaps").setMaster("local[*]"); // local[*] means to run spark locally and
                                                                                       // and assign all the threads for parallel execution
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> javaRDD = sc.parallelize(data);

        System.out.println(" *********** FLAT MAP *****************");
        JavaRDD<String> flatMapRDD = javaRDD.flatMap(a -> Arrays.asList(a.split(" ")).iterator());
        flatMapRDD.foreach(a-> System.out.println(a));

        /**
         *
         * Filter -> iterate through every element in the RDD
         * if we return true for a filtering condition, it will become part of the new RDD.
         * if we return false, the word will be filtered out
         * Gets rid of junk words
         *
        */

        System.out.println(" *********** FILTERED FLAT MAP *****************");
        JavaRDD<String> filteredRDD = flatMapRDD.filter(a -> a.length() > 1);
        filteredRDD.foreach(a-> System.out.println(a));
        sc.close();
    }
}
