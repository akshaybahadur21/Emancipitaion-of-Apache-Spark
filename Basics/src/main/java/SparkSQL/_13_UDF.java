package SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class _13_UDF {
    /**
     * User Defined Functions : Adding columns in a dataset based on some calculated values from the existing columns
     *
     */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Akshay GitHub\\winutils-master\\hadoop-2.7.1");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]")
                .config("spark.sql.warehouse", "file:///C:/Akshay Github/tmp")
                .getOrCreate();


        Dataset<Row> dataset = spark.read().option("header", true).csv("src\\main\\resources\\exams\\students.csv");
        dataset.createOrReplaceTempView("student_table");

        /**
         * withColumn() : Allows you to add a column.
         * */
        dataset.withColumn("Pass",lit("YES")).show(); // every cell element of Pass will be YES

        dataset.withColumn("Pass",lit(col("grade"))).show(); // every cell element will be filled from grade column

        dataset.withColumn("Pass",lit(col("grade").equalTo("A+"))).show(); // if grade == A+ -> TRUE else false

        /**
         * withColumn() is pretty difficult when you have some complex logic for adding a column
         * therefore we have UDF (User Defined Function)
         * For calling it, you need to register it in your spark session.
         *      - "hasPassed" : name of the function
         *      - define the function either using lambda or calling it old school way
         *      - DataTypes.BooleanType : Define the return type of the UDF in Spark terms
         *
         *  In the below code we see 3 types of UDF call
         *      - inline lambda
         *      - lambda
         *      - functions
         * */
        spark.udf().register("hasPassedLambda", (String grade) -> grade.equals("A+"), DataTypes.BooleanType);

        //creating a new UDF for pass -> if subject is biology, pass is A or A+ otherwise pass is C.
        spark.udf().register("hasPassed", (String grade, String subject) -> {
            if(subject.equals("Biology"))
                return grade.startsWith("A");
            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
        }, DataTypes.BooleanType);

        spark.udf().register("hasPassedFunction", hasPassedFunction, DataTypes.BooleanType);

        dataset.withColumn("Pass",callUDF("hasPassedLambda", col("grade"))).show();
        dataset.withColumn("Pass",callUDF("hasPassed", col("grade"), col("subject"))).show();
        dataset.withColumn("Pass",callUDF("hasPassedFunction", col("grade"), col("subject"))).show();

        /**
         * Let's go back to biglog and see how udf can be easily applied to sql and not JAVA apis
         * */
        dataset = spark.read().option("header", true).csv("src\\main\\resources\\biglog.txt.txt");

        SimpleDateFormat input = new SimpleDateFormat("MMMM");
        SimpleDateFormat output = new SimpleDateFormat("M");

        spark.udf().register("monthNum", (String month) -> {
            if(month == null) return 0;
            java.util.Date inputDate = input.parse(month);
            return Integer.parseInt(output.format(inputDate));
        }, DataTypes.IntegerType);


        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> results = spark.sql
                ("select level, date_format(datetime,'MMMM') as month, count(1) as total " +
                        "from logging_table group by level, month order by monthNum(month), level");
        results.show();
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        spark.close();
    }
    private static UDF2<String, String, Boolean> hasPassedFunction = new UDF2<String, String, Boolean>() {
        @Override
        public Boolean call(String grade, String subject) throws Exception {
            if(subject.equals("Biology"))
                return grade.startsWith("A");
            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
        }
    };
}
