package basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class F_LoadFromDisk {
    /**
     *
     * Loading from Disk
     * We can load data from HDFS or s3 if it is really big.
     * The entire dataset won't be loaded because that will crash the RAM
     * instead, the data is loaded in chunks/partition.
     * For now, we will use local file
     *
     * */

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("LoadFromDisk").setMaster("local[*]"); // local[*] means to run spark locally and
                                                                                       // and assign all the threads for parallel execution
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> fromDiskRDD = sc.textFile("src/main/resources/subtitles/input.txt"); // this can be from hdfs or s3 (big data)

        System.out.println(" *********** FLAT MAP *****************");
        JavaRDD<String> flatMapRDD = fromDiskRDD.flatMap(a -> Arrays.asList(a.split(" ")).iterator());
        flatMapRDD.foreach(a-> System.out.println(a));

    }
}
