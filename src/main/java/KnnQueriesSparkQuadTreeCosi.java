///**
// * Created by Wera on 2016-11-13.
// */

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Scanner;


public class KnnQueriesSparkQuadTreeCosi {

    private static String FILE_PATH = "C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/";
    private static String FILE_PATH_QUADTREE_DATA = "C:/Users/Wera/Documents/4thyear/IP/QuadTreeData/";
    private static String FILE_NAME_DATASET = FILE_PATH + "1000";
    private static String FILE_NAME_QUERY_POINTS = FILE_PATH + "10";
    private static int k = 5;

    public static void main( String[] args ){

        long startTime = System.currentTimeMillis();

        JavaSparkContext sc = sparkConfigSetUp();

        cleanQuadTreeDataDirectory();

        QuadTreeArray quadTree = buildQuadTree();

        quadTree.makeQuadTreeSummary();

        LinkedList<Point> queryPoints = loadQueryPoints();

        long startQueryTime = System.currentTimeMillis();

        for (Point queryPoint : queryPoints){
            System.out.println(queryPoint.getX() + " " + queryPoint.getY());
            nnQuery(quadTree, queryPoint.getX(), queryPoint.getY(), sc);
            System.out.println();
            System.out.println();
        }

        long estimatedQueryResponseTime = System.currentTimeMillis() - startQueryTime;
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("Total time :" + estimatedTime + " miliseconds");
        System.out.println("Query response time: " + estimatedQueryResponseTime + " miliseconds");
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
        return queryPoints;
    }

    private static QuadTreeArray buildQuadTree() {
        QuadTreeArray quadTree = new QuadTreeArray(0,0,1000000,1000000);
//        populate quadtree
        try {
            Scanner in = new Scanner(new FileReader(FILE_NAME_DATASET));
            while(in.hasNext()) {
                String[] data = in.nextLine().split(",");
                quadTree.set(Double.parseDouble(data[0]), Double.parseDouble(data[1]), null);
            }
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

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//        List<Neighbour> nearestNeighbours = findNearestNeighbours(mainPartition, x ,y);
//        Circle circle = findFurthestNeighbourCircle( x, y, nearestNeighbours);
//
//        LinkedList<NodeArray> partitons = quadTree.findPartitions(quadTree.getRootNodeArray(), circle, mainPartition);
//
//        for ( NodeArray partition : partitons){
//            nearestNeighbours.addAll(findNearestNeighbours(partition, x ,y));
//        }
//
//        Collections.sort(nearestNeighbours, new NeighbourComparator());
//
//        for( int i = 0; i < k ; i++){
//            System.out.println(i+1 + ") " + nearestNeighbours.get(i).toString());
//        }
    }


    private static Circle findFurthestNeighbourCircle(double x_coordinate, double y_coordinate, JavaRDD<Tuple3<Double,Double,Double>>  nearestNeighbours) {
        double distance = 0;
        for (Tuple3 s : nearestNeighbours.take(k)){
//            System.out.println("Point: " + s._1() + " " + s._2() + " " + s._3());
            distance = (double) s._3();
        }
//        System.out.println("Radious: " + distance);
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


