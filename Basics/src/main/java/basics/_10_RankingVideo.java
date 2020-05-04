package basics;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class _10_RankingVideo {
    /**
     *
     * This is a practise problem.
     * We have 2 log files ->
     * viewData.log has userID against the ChapterID.
     * chapterData.log has ChapterID against CourseID.
     *
     * End Goal : Identify which 'COURSES' are popular and which are not popular.
     *
     * */
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","");
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("RankingVideo").setMaster("local[*]"); // local[*] means to run spark locally and
        // and assign all the threads for parallel execution
        JavaSparkContext sc = new JavaSparkContext(conf);

        boolean testMode = false;
        JavaPairRDD<Integer, Integer> chapterRDD = setupChapterData(sc, testMode);
        JavaPairRDD<Integer, Integer> viewRDD = setupViewData(sc, testMode);

        /**
         *
         * WarmUp : We want to build an RDD which has courseID against the number of chapters
         *                  : <chapterID, courseID> -> <courseID, NUMBER_OF_CHAPTERS>
         * */

        JavaPairRDD<Integer, Integer> warmUpRDD = chapterRDD.mapToPair(a -> new Tuple2<>(a._2, 1)); //make <chapterID, courseID> -> <courseID, chapterID>
        warmUpRDD.countByKey()// or you can do reduceByKey like this ->  warmUpRDD.reduceByKey((a,b) -> a + b);
                .forEach((k, v) ->System.out.println(k + " ---> " +v));
        warmUpRDD = warmUpRDD.reduceByKey((a,b) -> a +b);

        /**
         *
         * Main Event
         * Business Rules:
         *    We think that if a user sticks it through most of the course, that's more
         *    deserving of "points" than if someone bails out just a quarter way through the
         *    course. So we've cooked up the following scoring system:
         *
         *       If a user watches more than 90% of the course, the course gets 10 points
         *       If a user watches > 50% but <90% , it scores 4
         *       If a user watches > 25% but < 50% it scores 2
         *       Less than 25% is no score
         *
         * */

        // Step 1 : Remove Duplicate Views
        viewRDD = viewRDD.distinct();

        //Step 2 : Join to get courseID -> <chapterID, <userID, courseID>>
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRDD = viewRDD.mapToPair(a -> a.swap())
                .join(chapterRDD);

        //step 3 : Remove chapterID and get count -> <<userID, courseID> , 1>
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> userCourseRDD = joinedRDD.mapToPair(a -> new Tuple2<>(a._2, 1));

        //Step 4 : Count Unique views for <<userID, courseID>, VIEWS>
        userCourseRDD = userCourseRDD.reduceByKey((a,b) -> a + b);

        //Step 5 : Drop the userID -> <courseID, VIEWS>
        JavaPairRDD<Integer, Integer> courseViewsRDD = userCourseRDD.mapToPair(a -> new Tuple2<>(a._1._2, a._2));

        // Step 6 : Get Total Number of Views added -> <courseID, <VIEWS, TOTAL_VIEWS>>
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> courseViewsTotalViewsRDD = courseViewsRDD.join(warmUpRDD);

        //Step 7 : Calculate percentages -> <courseID, PERCENTAGE>

        JavaPairRDD<Integer, Double> coursePercentageRDD = courseViewsTotalViewsRDD.mapToPair(a -> new Tuple2<Integer, Double>(a._1, (double)a._2._1 / (double)a._2._2));

        //Step 8 : Give Scoring according to Percentage

        JavaPairRDD<Integer, Long> courseScoreRDD = coursePercentageRDD.mapValues(a -> {
            if (a > 0.9) return 10L;
            if (a > 0.5) return 4L;
            if (a > 0.25) return 2L;
            else return 0L;
        });

        //Step 9 : Add the scores

        courseScoreRDD = courseScoreRDD.reduceByKey((a,b) -> a + b);

        // Additional Steps : Get CourseName as well

        JavaPairRDD<Integer, String> idNameRDD = setupCourseNameData(sc, testMode);

        courseScoreRDD.join(idNameRDD) // <id, score, name>
                .mapToPair(a -> new Tuple2<>(a._2._1, a._2._2))
                .sortByKey(false)
                .mapToPair(a -> a.swap())
                .collect()
                .forEach(System.out::println);

        sc.close();
    }

    private static JavaPairRDD<Integer, String> setupCourseNameData(JavaSparkContext sc, boolean testMode) {
        return sc.textFile("src\\main\\resources\\viewing figures\\titles.csv")
                .mapToPair(line ->{
                    String[] cols = line.split(",");
                    return new Tuple2<>(new Integer(cols[0]), new String(cols[1]));
                });
    }

    private static JavaPairRDD<Integer, Integer> setupChapterData(JavaSparkContext sc, boolean testMode) {
        if (testMode){
            //return a shorter version of data
            //<chapterID, courseID>
            List<Tuple2<Integer, Integer>> testData = new ArrayList<>();
            testData.add(new Tuple2<>(96,1));
            testData.add(new Tuple2<>(97,1));
            testData.add(new Tuple2<>(98,1));
            testData.add(new Tuple2<>(99,2));
            testData.add(new Tuple2<>(100,3));
            testData.add(new Tuple2<>(101,3));
            testData.add(new Tuple2<>(102,3));
            testData.add(new Tuple2<>(103,3));
            testData.add(new Tuple2<>(104,3));
            testData.add(new Tuple2<>(105,3));
            testData.add(new Tuple2<>(106,3));
            testData.add(new Tuple2<>(107,3));
            testData.add(new Tuple2<>(108,3));
            testData.add(new Tuple2<>(109,3));
            return sc.parallelizePairs(testData);
        }
        return sc.textFile("src\\main\\resources\\viewing figures\\chapters.csv")
                .mapToPair(line ->{
                    String[] cols = line.split(",");
                    return new Tuple2<>(new Integer(cols[0]), new Integer(cols[1]));
                });
    }

    private static JavaPairRDD<Integer, Integer> setupViewData(JavaSparkContext sc, boolean testMode) {
        if (testMode){
            //return a shorter version of data
            //userID, chapterID
            List<Tuple2<Integer, Integer>> testData = new ArrayList<>();
            testData.add(new Tuple2<>(14,96));
            testData.add(new Tuple2<>(14,97));
            testData.add(new Tuple2<>(13,96));
            testData.add(new Tuple2<>(13,96));
            testData.add(new Tuple2<>(13,96));
            testData.add(new Tuple2<>(14,99));
            testData.add(new Tuple2<>(13,100));

            return sc.parallelizePairs(testData);
        }
        return sc.textFile("src\\main\\resources\\viewing figures\\views-*.csv")
                .mapToPair(line ->{
                    String[] cols = line.split(",");
                    return new Tuple2<>(new Integer(cols[0]), new Integer(cols[1]));
                });
    }
}
