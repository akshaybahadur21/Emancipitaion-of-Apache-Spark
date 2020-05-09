package SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class _04_InMemoryInputData {
    /**
     *
     * In Memory Input Data
     * In the Spark Core module, we used sc.parallelize() method to directly parallelize data.
     * We don't really need the same with Spark SQL but we will need it for writing test cases.
     *
     * */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]")
                                        .config("spark.sql.warehouse","file:///C:/Akshay Github/tmp")
                                        .getOrCreate();
        /**
         * We want to create in memory list of Rows for PoC work
         * DataFrame = Dataset<Row>
         * createDataFrame
         *    - Returns a Dataset of Rows
         *    - Can take in RDD, List and return a dataset
         *    - We must pass a SCHEMA so that spark knows the datatype
         *    - We must create the schema
         *
         * */
        List<Row> inMemory = new ArrayList<>();

        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

        StructField[] fields = new StructField[] {
                new StructField("Level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);
        dataset.show();

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
