package SparkML;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

import static org.apache.spark.sql.functions.col;

public class _01_Introduction {
    /**
     *
     * Getting Started with Spark MLLib
     * SparkMLLib provides 2 types of API
     *      - Spark SQL
     *      - Spark Core (In Maintenance since Spark 2.3)
     * Since the Spark Core version is in maintenance, we will work with Spark SQL version
     *
     * */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark MLLib").master("local[*]")
                                        .config("spark.sql.warehouse","file:///C:/Akshay Github/tmp")
                                        .getOrCreate();
        Dataset<Row> dataset = spark.read().option("header", true).option("inferSchema",true).csv("src\\main\\resources\\GymCompetition.csv");
        dataset.show();
        /**
         * 1. Manipulate the dataset into features and labels format
         * Total columns in csv
         * +------------+------+---+------+------+--------+
         * |CompetitorID|Gender|Age|Height|Weight|NoOfReps|
         * +------------+------+---+------+------+--------+
         *
         * 2. We want to select Age, Height and Weight as features and NoOfReps as Label.
         * These features are Spark vectors(like an array in spark)
         * For doing this, we call VectorAssembler which creates features from our dataset.
         * VectorAssembler only takes in Columns which are in Number format
         * */
        VectorAssembler vectorAssembler = new VectorAssembler();
        vectorAssembler.setInputCols(new String[]{"Age", "Height", "Weight"});
        vectorAssembler.setOutputCol("features");
        dataset = vectorAssembler.transform(dataset);
        dataset.show();
        dataset.printSchema();

        /**
         * 3. Since we are only using features and NoOfReps, we can remove all the other columns
         * */
        Dataset<Row> inputDataset = dataset.select(col("features"), col("NoOfReps")).withColumnRenamed("NoOfReps", "label");
        inputDataset.show();

        /**
         * 4. Build the Linear Regression Model
         * */

        LinearRegression linearRegression = new LinearRegression();
        LinearRegressionModel model = linearRegression.fit(inputDataset);
        System.out.println("Intercept : " + model.intercept()); //bias
        System.out.println("Coefficient : " + model.coefficients()); //coefficients

        /**
         * 5. Check if the model works or not
         * */
        model.transform(inputDataset).show();
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
