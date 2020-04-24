package basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class _1_Reduce {

    public static void main(String[] args) {
        List<Double> data = new ArrayList<>();
        data.add(1.4);
        data.add(4.4);
        data.add(1.7);
        data.add(11.56);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("Reduce").setMaster("local[*]"); // local[*] means to run spark locally and
                                                                                         // and assign all the threads for parallel execution
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Double> javaRDD = sc.parallelize(data);
        Double check = javaRDD.reduce(new SampleFunction());
        Double res = javaRDD.reduce((a, b) -> a + b); // reduce the values by adding them (types of 'a' and 'b' are inferred)
        System.out.println("************   ************");
        System.out.println(check);
        System.out.println("************   ************");
        System.out.println(res);

        sc.close(); //Necessary to close the spark context. Good practise it to close it in the
                    // finally block and put everything else in try block
    }

    private static class SampleFunction implements org.apache.spark.api.java.function.Function2<Double, Double, Double> {
        @Override
        public Double call(Double aDouble, Double aDouble2) throws Exception {
            return aDouble + aDouble2;
        }
    }
}
