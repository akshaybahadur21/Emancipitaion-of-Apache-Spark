package SparkML;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

import java.util.Scanner;

public class _04_Feature_Selection {
    /**
     *
     * Feature Selection : This requires in-depth knowledge. We shall only look at the basics
     *      - Eliminate Dependent variable
     *      - Range of values of each variable (can use spark describe() method)
     *      - Features which had good potential as predictors (High Correlation)
     *      - removing duplicate features or highly correlated features
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

        dataset.describe().show();

        /**
         * Spark can let us know correlation between features
         *      -1 and 1 mean high correlation
         *      0 has no correlation
         * Use stat() function
         * */

        dataset = dataset.drop("id", "date", "waterfront", "view", "condition", "grade", "yr_renovated", "zipcode", "lat", "long");

        for(String col : dataset.columns())
            System.out.println("Correlation b/w price and "+ col + " : " +  dataset.stat().corr("price", col));
        dataset = dataset.drop("sqft_lot", "yr_built", "sqft_lot15, sqft_living15"); // removing these after getting low correlation

        dataset = dataset.withColumn("sqft_above_precentage", col("sqft_above").divide("sqft_living"));
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setHandleInvalid("skip")
                .setInputCols(new String[]{"bedrooms", "bathrooms", "sqft_living", "sqft_above_precentage", "floors"})
                .setOutputCol("features");
        dataset = vectorAssembler.transform(dataset);

        Dataset<Row> inputDataset = dataset.select(col("features"), col("price")).withColumnRenamed("price", "label");

        Dataset<Row>[] trainTestData = inputDataset.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainData = trainTestData[0];
        Dataset<Row> testData = trainTestData[1];

        LinearRegression linearRegression = new LinearRegression();
        ParamGridBuilder paramGridBuilder = new ParamGridBuilder();
        ParamMap[] paramMaps = paramGridBuilder.addGrid(linearRegression.regParam(), new double[]{0.01, 0.1, 0.5})
                .addGrid(linearRegression.elasticNetParam(), new double[]{0.0, 0.5, 1.0})
                .build();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(linearRegression)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                .setEstimatorParamMaps(paramMaps)
                .setTrainRatio(0.8);

        TrainValidationSplitModel splitModel = trainValidationSplit.fit(trainData);
        LinearRegressionModel model = (LinearRegressionModel) splitModel.bestModel();
        System.out.println("Intercept : " + model.intercept()); //bias
        System.out.println("Coefficient : " + model.coefficients()); //coefficients
        System.out.println("R2 value for training data : " + model.summary().r2());
        System.out.println("RMSE value for training data : " + model.summary().rootMeanSquaredError());

        System.out.println("R2 value for test data : " + model.evaluate(testData).r2());
        System.out.println("RMSE value for test data : " + model.evaluate(testData).rootMeanSquaredError());
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
}
