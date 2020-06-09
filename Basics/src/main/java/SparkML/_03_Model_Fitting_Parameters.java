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

public class _03_Model_Fitting_Parameters {
    /**
     *
     * Model Fitting Parameters : This is in continuation to to our last module on House Sale Prediction
     * We saw a WARN : WeightedLeastSquares
     * Till now the LinearRegression class is a black box for us.
     * We can provide these optional parameters and see which one works the best for us
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

        VectorAssembler vectorAssembler = new VectorAssembler()
                        .setInputCols(new String[]{"bedrooms", "bathrooms", "sqft_living", "sqft_lot", "floors", "grade"})
                        .setOutputCol("features");
        dataset = vectorAssembler.transform(dataset);

        Dataset<Row> inputDataset = dataset.select(col("features"), col("price")).withColumnRenamed("price", "label");

        Dataset<Row>[] trainTestData = inputDataset.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainData = trainTestData[0];
        Dataset<Row> testData = trainTestData[1];

        /**
         * We can specify a grid of values and spark will the best one out of them using ParamGridBuilder
         * We also need to split the entire dataset to train : test : validate.
         * Spark will do that for use. We need to create a TrainValidationSplit Object
         *      - estimator : Model which we are using.
         *      - evaluator : how to determine the best model
         *      - param Map : the map for all the possible values for param
         *      - train ratio : what proportion to split into training and test.
         * */

//        LinearRegression linearRegression = new LinearRegression()
//                                                .setMaxIter(10)
//                                                .setRegParam(0.3)
//                                                .setElasticNetParam(0.8);

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

//        Scanner scanner = new Scanner(System.in);
//        scanner.nextLine();
        spark.close();
    }
}
