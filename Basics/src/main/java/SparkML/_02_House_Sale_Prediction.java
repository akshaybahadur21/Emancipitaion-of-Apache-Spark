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

public class _02_House_Sale_Prediction {
    /**
     *
     * Linear Regression on House Sale Dataset
     * https://www.kaggle.com/harlfoxem/housesalesprediction
     *
     * */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\Akshay_GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark MLLib").master("local[*]")
                                        .config("spark.sql.warehouse","file:///C:/Akshay Github/tmp")
                                        .getOrCreate();
        Dataset<Row> dataset = spark.read().option("header", true).option("inferSchema",true).csv("src\\main\\resources\\kc_house_data.csv");
        dataset.show();

        /**
         * For this usecase, we will only work with 3 columns, bedroom, bathrooms and sqft_living
         * */
        VectorAssembler vectorAssembler = new VectorAssembler()
                        .setInputCols(new String[]{"bedrooms", "bathrooms", "sqft_living", "sqft_lot", "floors", "grade"})
                        .setOutputCol("features");
        dataset = vectorAssembler.transform(dataset);
        dataset.show();
        dataset.printSchema();

        /**
         * Since we are only using features and price, we can remove all the other columns
         * */
        Dataset<Row> inputDataset = dataset.select(col("features"), col("price")).withColumnRenamed("price", "label");
        inputDataset.show();

       /**
        * We can think of splitting the data since we are working with 21,000 rows in 80% - 20%
        * */

        Dataset<Row>[] trainTestData = inputDataset.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainData = trainTestData[0];
        Dataset<Row> testData = trainTestData[1];

        LinearRegression linearRegression = new LinearRegression();
        LinearRegressionModel model = linearRegression.fit(trainData);
        System.out.println("Intercept : " + model.intercept()); //bias
        System.out.println("Coefficient : " + model.coefficients()); //coefficients
        System.out.println("R2 value for training data : " + model.summary().r2());
        System.out.println("RMSE value for training data : " + model.summary().rootMeanSquaredError());

        /**
         * How ro check the efficieny of Model
         *  - RMSE (Root Mean Squared Error) :
         *  - R-squared :
         * */
        model.transform(testData).show();
        System.out.println("R2 value for test data : " + model.evaluate(testData).r2());
        System.out.println("RMSE value for test data : " + model.evaluate(testData).rootMeanSquaredError());

//        Scanner scanner = new Scanner(System.in);
//        scanner.nextLine();
        spark.close();
    }
}
