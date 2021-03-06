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

import java.io.*;
import java.util.LinkedList;
import java.util.Scanner;


public class KnnQueriesSparkCore {

    private static String FILE_PATH = "C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/";
    private static String FILE_PATH_OUTPUT = "C:/Users/Wera/Documents/4thyear/IP/time_output_SparkCore.txt";
    private static String DATA_TYPE = "normal";
    private static String DATASET = "10000000";
    private static String QUERY_POINTS = "100";
    private static String FILE_NAME_DATASET = FILE_PATH + DATASET;
    private static String FILE_NAME_QUERY_POINTS = FILE_PATH + QUERY_POINTS;
    private static int k = 1000;


    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

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

//            distancesToPoints.saveAsTextFile(FILE_PATH_OUTPUT + "SparkCore.txt");
//            distancesToPoints.coalesce(1).saveAsTextFile(FILE_PATH_OUTPUT + "test.txt");

            System.out.println();
            System.out.println();
        }

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(estimatedTime + " miliseconds");

        saveTimeMeasurementsToFile(estimatedTime);
    }

    private static void saveTimeMeasurementsToFile(long estimatedTime) {
        StringBuilder timeOutput = new StringBuilder();
        timeOutput.append(DATA_TYPE).append(",")
                .append(DATASET).append(",")
                .append(QUERY_POINTS).append(",")
                .append(k).append(",")
                .append(estimatedTime).append(",")
                .append(System.getProperty("line.separator"));


        File file = new File(FILE_PATH_OUTPUT);

        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(file, true));
            if(file.length() == 0) {
                StringBuilder columnNames = new StringBuilder();
                columnNames.append("DATA_TYPE").append(",")
                        .append("DATASET").append(",")
                        .append("QUERY_POINTS").append(",")
                        .append("k").append(",")
                        .append("estimatedQueryResponseTime").append(",")
                        .append(System.getProperty("line.separator"));
                bw.write(columnNames.toString());
            }
            bw.append(timeOutput.toString());
            bw.flush();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            try {
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}