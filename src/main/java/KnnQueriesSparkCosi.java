///**
// * Created by Wera on 2016-11-13.
// */

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

public class KnnQueriesSparkCosi {
    public static void main(String[] args) {

//        number of nn queries to find
        int k = 5;

//        the coordinates of the main point of reference
        double x_coordinate = 0;
        double y_coordinate = 0;

//        disable the log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("knn_queries_spark_core").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> pointsfromFileString = sc.textFile("C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/10.csv");
    }
}