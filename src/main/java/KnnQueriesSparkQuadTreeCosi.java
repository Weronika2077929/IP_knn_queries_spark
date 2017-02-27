///**
// * Created by Wera on 2016-11-13.
// */

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Boolean;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.util.LinkedList;
import java.util.Scanner;


public class KnnQueriesSparkQuadTreeCosi {

    private static String FILE_PATH = "C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/";
    private static String FILE_PATH_QUADTREE_DATA = "C:/Users/Wera/Documents/4thyear/IP/QuadTreeData/";
    private static String DATA_TYPE = "normal";
    private static String DATASET = "10000000";
    private static String QUERY_POINTS = "10";
    private static String FILE_NAME_DATASET = FILE_PATH + DATASET;
    private static String FILE_NAME_QUERY_POINTS = FILE_PATH + QUERY_POINTS;
    private static String TIME_MEASUREMENT_OUTPUT_FILE = "C:/Users/Wera/Documents/4thyear/IP/time_output.txt";
    private static boolean RECREATE_QUADTREE = true;
    private static int k = 5;

    public static void main( String[] args ){

        long startTotalTime = System.currentTimeMillis();

        JavaSparkContext sc = sparkConfigSetUp();

        QuadTreeArray quadTree = new QuadTreeArray(0,0,1000000,1000000);


        long startQuadTreeBuildingTime = System.currentTimeMillis();

        if( RECREATE_QUADTREE ) {
            quadTree = recreateQuadTreeFromSumamryFile(quadTree);
        } else {
            cleanQuadTreeDataDirectory();
            quadTree = buildQuadTree(quadTree);
            quadTree.makeQuadTreeSummary();
        }
        long estimatedQuadTreeBuildingTime = System.currentTimeMillis() - startQuadTreeBuildingTime;

//        TODO put in array instead of LinkedList
        LinkedList<Point> queryPoints = loadQueryPoints();

        long startQueryTime = System.currentTimeMillis();

        for (Point queryPoint : queryPoints){
            System.out.println(queryPoint.getX() + " " + queryPoint.getY());
            nnQuery(quadTree, queryPoint.getX(), queryPoint.getY(), sc);
            System.out.println();
            System.out.println();
        }

        long estimatedQueryResponseTime = System.currentTimeMillis() - startQueryTime;
        long estimatedTotalTime = System.currentTimeMillis() - startTotalTime;
        System.out.println("Total time: " + estimatedTotalTime + " miliseconds");
        System.out.println("Query response time: " + estimatedQueryResponseTime + " miliseconds");
        System.out.println("Building QuadTree time: " + estimatedQuadTreeBuildingTime + " miliseconds");

        saveTimeMeasurementToFile(estimatedQueryResponseTime, estimatedTotalTime, estimatedQuadTreeBuildingTime, quadTree);
    }

    private static void saveTimeMeasurementToFile(long estimatedQueryResponseTime, long estimatedTime, long estimatedQuadTreeBuildingTime, QuadTreeArray quadTree) {


        StringBuilder timeOutput = new StringBuilder();
        timeOutput.append(DATA_TYPE).append(",")
                .append(DATASET).append(",")
                .append(QUERY_POINTS).append(",")
                .append(quadTree.getRootNodeArray().NODE_CAPACITY).append(",")
                .append(k).append(",")
                .append(RECREATE_QUADTREE).append(",")
                .append(estimatedTime).append(",")
                .append(estimatedQueryResponseTime).append(",")
                .append(estimatedQuadTreeBuildingTime).append(",")
                .append(System.getProperty("line.separator"));


        File file = new File(TIME_MEASUREMENT_OUTPUT_FILE);

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
                        .append("NODE_CAPACITY").append(",")
                        .append("k").append(",")
                        .append("RECREATE_QUADTREE").append(",")
                        .append("estimatedTime").append(",")
                        .append("estimatedQueryResponseTime").append(",")
                        .append("estimatedQuadTreeBuildingTime").append(",")
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


    private static QuadTreeArray recreateQuadTreeFromSumamryFile(QuadTreeArray quadTree) {
        quadTree.createQuadTreeFromSummaryFile(FILE_PATH_QUADTREE_DATA + "summary.txt");

        return quadTree;
    }

    private static JavaSparkContext sparkConfigSetUp() {
        //        disable the log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("knn_queries_spark_core").setMaster("local");
        return new JavaSparkContext(conf);
    }

    private static LinkedList<Point> loadQueryPoints() {

//        read all the query points from the file
        LinkedList<Point> queryPoints = new LinkedList<>();

        try {
            BufferedReader in = new BufferedReader(new FileReader(FILE_NAME_QUERY_POINTS));
            in.lines().forEach(line ->{
                String[] data = line.split(",");
                queryPoints.add(new Point(Double.parseDouble(data[0]), Double.parseDouble(data[1]), null));
            } );
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return queryPoints;
    }

    private static QuadTreeArray buildQuadTree(QuadTreeArray quadTree) {

        try {
            BufferedReader in = new BufferedReader(new FileReader(FILE_NAME_DATASET));
            in.lines().forEach(line -> {
                String[] data = line.split(",");
                quadTree.set(Double.parseDouble(data[0]), Double.parseDouble(data[1]), null);
            });
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        quadTree.saveQuadTreetoDisk();
        return quadTree;
    }

    private static void cleanQuadTreeDataDirectory() {
        try {
            FileUtils.cleanDirectory(new File(FILE_PATH_QUADTREE_DATA));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void nnQuery(QuadTreeArray quadTree, double x, double y, JavaSparkContext sc) {
        NodeArray mainPartition = quadTree.findPariton(x, y);

        JavaRDD<String> pointsfromFileString = sc.textFile( mainPartition.getFileName());

        JavaRDD<Tuple2<Double,Double>> pointsfromFileTuple = pointsfromFileString.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.apply(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]));
        });

        JavaRDD<Tuple3<Double,Double,Double>> distancesToPoints = pointsfromFileTuple.map(s -> {
            double distance = Math.sqrt(Math.pow(x - s._1, 2) + Math.pow(y - s._2, 2));

            return new Tuple3<Double, Double, Double>(s._1, s._2, distance);
        });

        distancesToPoints = distancesToPoints.sortBy(z -> z._3(), true, 1);

        Circle circle = findFurthestNeighbourCircle(x,y,distancesToPoints);

        LinkedList<NodeArray> partitons = quadTree.findPartitions(quadTree.getRootNodeArray(), circle, mainPartition);

        for ( NodeArray partition : partitons){

            distancesToPoints = distancesToPoints.union(findNearestNeighbours(partition,x,y,sc));
        }

        distancesToPoints = distancesToPoints.sortBy(z -> z._3(), true, 1);

        System.out.println();
        for (Tuple3 s : distancesToPoints.take(k)){
            System.out.println(s);
        }
    }


    private static Circle findFurthestNeighbourCircle(double x_coordinate, double y_coordinate, JavaRDD<Tuple3<Double,Double,Double>>  nearestNeighbours) {
        double distance = 0;
        for (Tuple3 s : nearestNeighbours.take(k)){
            distance = (double) s._3();
        }
        return new Circle(x_coordinate, y_coordinate, distance);
    }

    private static JavaRDD<Tuple3<Double, Double, Double>> findNearestNeighbours(NodeArray partition, double x_coordinate, double y_coordinate, JavaSparkContext sc) {

        JavaRDD<String> pointsfromFileString = sc.textFile( partition.getFileName());

        JavaRDD<Tuple2<Double,Double>> pointsfromFileTuple = pointsfromFileString.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.apply(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]));
        });

        JavaRDD<Tuple3<Double,Double,Double>> distancesToPoints = pointsfromFileTuple.map(s -> {
            double distance = Math.sqrt(Math.pow(x_coordinate - s._1, 2) + Math.pow(y_coordinate - s._2, 2));

            return new Tuple3<Double, Double, Double>(s._1, s._2, distance);
        });

        distancesToPoints = distancesToPoints.sortBy(z -> z._3(), true, 1);

        return distancesToPoints;
    }
}


