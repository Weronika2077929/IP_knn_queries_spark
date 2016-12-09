///**
// * Created by Wera on 2016-11-13.
// */

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KnnQueriesSparkCore {
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

        JavaRDD<String> distFile = sc.textFile("C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/10.csv");


//        JavaRDD<Double[]> map = distFile.map(s -> {
//            String[] arr = s.split(",");
//            Double[] doubleArr = new Double[arr.length];
//            for(int i = 0; i < arr.length; i++) {
//                doubleArr[i] = Double.parseDouble(arr[i]);
//            }
//            return doubleArr;
//        });


        JavaRDD<Tuple2<Double,Double>> map = distFile.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.apply(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]));
        });

        for( Tuple2 s : map.collect()){
            System.out.println(s._1 + " " + s._2);
        }

        JavaRDD<Double> map2 = map.map(s -> Math.sqrt(Math.pow(x_coordinate - s._1, 2) + Math.pow(y_coordinate - s._2 , 2)));

        System.out.println();
        for( Double s : map2.collect()){
            System.out.println(s);
        }

    }
}