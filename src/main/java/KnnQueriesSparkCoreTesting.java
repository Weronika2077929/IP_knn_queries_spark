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

import java.io.*;
import java.util.LinkedList;
import java.util.Scanner;


public class KnnQueriesSparkCoreTesting {

    private static String FILE_PATH = "C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/";
    private static String FILE_PATH_OUTPUT = "C:/Users/Wera/Documents/4thyear/IP/time_output_SparkCore.txt";
    private static String DATA_TYPE = "normal";
    private String DATASET;
    private String QUERY_POINTS;
    private String FILE_NAME_DATASET ;
    private String FILE_NAME_QUERY_POINTS;
    private int k;

    public KnnQueriesSparkCoreTesting(String DATASET, String QUERY_POINTS, int k) {
        this.DATASET = DATASET;
        this.QUERY_POINTS = QUERY_POINTS;
        this.k = k;
        this.FILE_NAME_DATASET = FILE_PATH + DATASET;
        this.FILE_NAME_QUERY_POINTS = FILE_PATH + QUERY_POINTS;

    }

    public static void main(String[] args) {

        JavaSparkContext sc = setUpConfig();

//        KnnQueriesSparkCoreTesting test = new KnnQueriesSparkCoreTesting("10000000", "100", 10);
//        test.run(sc);
//        KnnQueriesSparkCoreTesting test2 = new KnnQueriesSparkCoreTesting("10000000", "100", 100);
//        test2.run(sc);

//        KnnQueriesSparkCoreTesting test3 = new KnnQueriesSparkCoreTesting("10000000", "100", 1000);
//        test3.run(sc);


//
//        KnnQueriesSparkCoreTesting test4 = new KnnQueriesSparkCoreTesting("Gowalla_Sample.csv", "100_smallerRange.csv", 10);
//        test4.run(sc);
//        KnnQueriesSparkCoreTesting test5 = new KnnQueriesSparkCoreTesting("Gowalla_Sample.csv", "100_smallerRange.csv", 100);
//        test5.run(sc);
//        KnnQueriesSparkCoreTesting test6 = new KnnQueriesSparkCoreTesting("Gowalla_Sample.csv", "100_smallerRange.csv", 1000);
//        test6.run(sc);

//        KnnQueriesSparkCoreTesting test7 = new KnnQueriesSparkCoreTesting("multivarTest.csv", "100", 10);
//        test7.run(sc);
//        KnnQueriesSparkCoreTesting test8 = new KnnQueriesSparkCoreTesting("multivarTest.csv", "100", 100);
//        test8.run(sc);
//        KnnQueriesSparkCoreTesting test9 = new KnnQueriesSparkCoreTesting("multivarTest.csv", "100", 1000);
//        test9.run(sc);
//
    }

    private static JavaSparkContext setUpConfig(){
        //        disable the log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("knn_queries_spark_core").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }

    private void run(JavaSparkContext sc) {

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

        long startTime = System.currentTimeMillis();
        JavaRDD<String> pointsfromFileString = sc.textFile(FILE_NAME_DATASET);

        JavaRDD<Tuple2<Double,Double>> pointsfromFileTuple = pointsfromFileString.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.apply(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]));
        });

        for (Point queryPoint : queryPoints){
//            System.out.println(queryPoint.getX() + " " + queryPoint.getY());

            JavaRDD<Tuple3<Double,Double,Double>> distancesToPoints = pointsfromFileTuple.map(s -> {
                double distance = Math.sqrt(Math.pow(queryPoint.getX() - s._1, 2) + Math.pow(queryPoint.getY() - s._2, 2));

                return new Tuple3<Double, Double, Double>(s._1, s._2, distance);
            });

            distancesToPoints = distancesToPoints.sortBy(x -> x._3(), true, 1);

//            System.out.println();
            for (Tuple3 s : distancesToPoints.take(k)){
//                System.out.println(s);
            }

//            System.out.println();
//            System.out.println();
        }

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(estimatedTime + " miliseconds");

        saveTimeMeasurementsToFile(estimatedTime);
    }

    private void saveTimeMeasurementsToFile(long estimatedTime) {
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