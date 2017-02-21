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
import scala.Tuple3;

import java.io.FileNotFoundException;
import java.io.FileReader;


public class KnnQueriesSparkCore {

    private static String FILE_PATH = "C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/";
    private static String FILE_PATH_QUADTREE_DATA = "C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/quadtree_data/";
    private static String FILE_NAME_DATASET = FILE_PATH + "1000";
    private static String FILE_NAME_QUERY_POINTS = FILE_PATH + "10";
    private static double x_coordinate = 0;
    private static double y_coordinate = 0;
    private static int k = 5;


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


        //        read all the query points from the file
        LinkedList<Point> queryPoints = new LinkedList<>();
//        Skips first line in the file
        try {
            Scanner in = new Scanner(new FileReader(FILE_NAME_QUERY_POINTS));
            while(in.hasNext()) {
                String[] data = in.nextLine().split(",");
                queryPoints.add(new Point(Double.parseDouble(data[0]), Double.parseDouble(data[1]), null));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


        for (Point queryPoint : queryPoints){
            System.out.println(queryPoint.getX() + " " + queryPoint.getY());

            JavaRDD<String> pointsfromFileString = sc.textFile(FILE_NAME_DATASET);

            JavaRDD<Tuple2<Double,Double>> pointsfromFileTuple = pointsfromFileString.map(s -> {
                String[] arr = s.split(",");
                return Tuple2.apply(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]));
            });

            JavaRDD<Tuple3<Double,Double,Double>> distancesToPoints = pointsfromFileTuple.map(s -> {
                double distance = Math.sqrt(Math.pow(queryPoint.getX() - s._1, 2) + Math.pow(queryPoint.getY() - s._2, 2));

                return new Tuple3<Double, Double, Double>(s._1, s._2, distance);
            });

            distancesToPoints = distancesToPoints.sortBy(x -> x._3(), true, 1);

            System.out.println();
            for (Tuple3 s : distancesToPoints.take(k)){
                System.out.println(s);
            }

            System.out.println();
            System.out.println();
        }

    }
}