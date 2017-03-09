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

import java.io.*;
import java.util.LinkedList;


public class KnnQueriesSparkQuadTreeIndex {

    private static String FILE_PATH = "C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/";
    private static String FILE_PATH_QUADTREE_DATA = "C:/Users/Wera/Documents/4thyear/IP/QuadTreeData/";
    private static String TIME_MEASUREMENT_OUTPUT_FILE = "C:/Users/Wera/Documents/4thyear/IP/time_outputTreeIndex.txt";
    private static String FILE_PATH_SUMMARY = "C:/Users/Wera/Documents/4thyear/IP/QuadTreeSummaries/";


    private static String DATA_TYPE = "normal";
    private String DATASET;
    private String QUERY_POINTS;
    private String FILE_NAME_DATASET;
    private String FILE_NAME_QUERY_POINTS;
    private boolean RECREATE_QUADTREE;
    private int k;
    private long datapointsAccessed;
    private long filesAccessed;
    private String summaryFile;
    private int nodeSize;
    private double identificationTime;
    private double fileAccessingTime;
    private double knnCalcualtionTime;
    private double verificationTime;
    private double resultsFetching;

    public KnnQueriesSparkQuadTreeIndex(String DATASET, String QUERY_POINTS, boolean RECREATE_QUADTREE, int k, int nodeSize) {
        this.DATASET = DATASET;
        this.QUERY_POINTS = QUERY_POINTS;
        this.RECREATE_QUADTREE = RECREATE_QUADTREE;
        this.k = k;
        this.nodeSize = nodeSize;
        this.FILE_NAME_DATASET = FILE_PATH + DATASET;
        this.FILE_NAME_QUERY_POINTS = FILE_PATH + QUERY_POINTS;
        this.datapointsAccessed = 0;
        this.filesAccessed = 0;
        this.summaryFile = "summary" + DATASET + ".txt";
    }

    public static void main(String[] args ){

        JavaSparkContext sc = sparkConfigSetUp();

//        KnnQueriesSparkQuadTreeIndex test = new KnnQueriesSparkQuadTreeIndex("10000000", "100", false, 10, 3000);
//        test.run(sc);
//        KnnQueriesSparkQuadTreeIndex test1 = new KnnQueriesSparkQuadTreeIndex("10000000", "100", true, 10 , 3000 );
//        test1.run(sc);
//        KnnQueriesSparkQuadTreeIndex test2 = new KnnQueriesSparkQuadTreeIndex("10000000", "100", true, 100, 3000 );
//        test2.run(sc);
//        KnnQueriesSparkQuadTreeIndex test3 = new KnnQueriesSparkQuadTreeIndex("10000000", "100", true, 1000, 3000 );
//        test3.run(sc);

//        KnnQueriesSparkQuadTreeIndex test10 = new KnnQueriesSparkQuadTreeIndex("10000000", "100", false, 10, 1000000);
//        test10.run(sc);
//        KnnQueriesSparkQuadTreeIndex test11 = new KnnQueriesSparkQuadTreeIndex("10000000", "100", true, 10, 1000000);
//        test11.run(sc);

//        KnnQueriesSparkQuadTreeIndex test8 = new KnnQueriesSparkQuadTreeIndex("10000000", "100", false, 10, 100000);
//        test8.run(sc);
//        KnnQueriesSparkQuadTreeIndex test9 = new KnnQueriesSparkQuadTreeIndex("10000000", "100", true, 10, 100000);
//        test9.run(sc);
//
////
//        KnnQueriesSparkQuadTreeIndex test6 = new KnnQueriesSparkQuadTreeIndex("10000000", "100", false, 10, 10000);
//        test6.run(sc);
//        KnnQueriesSparkQuadTreeIndex test7 = new KnnQueriesSparkQuadTreeIndex("10000000", "100", true, 10, 10000);
//        test7.run(sc);
//
//        KnnQueriesSparkQuadTreeIndex test4 = new KnnQueriesSparkQuadTreeIndex("10000000", "100", false, 10, 1000);
//        test4.run(sc);
//        KnnQueriesSparkQuadTreeIndex test5 = new KnnQueriesSparkQuadTreeIndex("10000000", "100", true, 10, 1000);
//        test5.run(sc);

        KnnQueriesSparkQuadTreeIndex test6 = new KnnQueriesSparkQuadTreeIndex("multivarTest.csv", "100", false, 10, 100);
        test6.run(sc);
//        KnnQueriesSparkQuadTreeIndex test7 = new KnnQueriesSparkQuadTreeIndex("multivarTest.csv", "100", true, 10, 100);
//        test7.run(sc);
//        KnnQueriesSparkQuadTreeIndex test8 = new KnnQueriesSparkQuadTreeIndex("multivarTest.csv", "100", true, 100, 100);
//        test8.run(sc);
//        KnnQueriesSparkQuadTreeIndex test9 = new KnnQueriesSparkQuadTreeIndex("multivarTest.csv", "100", true, 1000, 100);
//        test9.run(sc);

//        KnnQueriesSparkQuadTreeIndex test10 = new KnnQueriesSparkQuadTreeIndex("Gowalla_Sample.csv", "100_smallerRange", false, 10, 3000);
//        test10.run(sc);
//        KnnQueriesSparkQuadTreeIndex test11 = new KnnQueriesSparkQuadTreeIndex("Gowalla_Sample.csv", "100_smallerRange", true, 10, 3000);
//        test11.run(sc);
//        KnnQueriesSparkQuadTreeIndex test12 = new KnnQueriesSparkQuadTreeIndex("multivarTest.csv", "100", true, 100, 3000);
//        test12.run(sc);
//        KnnQueriesSparkQuadTreeIndex test13 = new KnnQueriesSparkQuadTreeIndex("multivarTest.csv", "100", true, 1000, 3000);
//        test13.run(sc);
    }

    private void run(JavaSparkContext sc) {
        long startTotalTime = System.currentTimeMillis();

//        QuadTreeArray quadTree = new QuadTreeArray(-200,-200,200,200, this.nodeSize);
        QuadTreeArray quadTree = new QuadTreeArray(0,0,1000000,1000000, this.nodeSize);

        long startQuadTreeBuildingTime = System.currentTimeMillis();

        if( RECREATE_QUADTREE ) {
            quadTree = recreateQuadTreeFromSumamryFile(quadTree, this.summaryFile);
        } else {
            cleanQuadTreeDataDirectory();
            quadTree = buildQuadTree(quadTree);
            quadTree.makeQuadTreeSummary(this.summaryFile);
        }
        long estimatedQuadTreeBuildingTime = System.currentTimeMillis() - startQuadTreeBuildingTime;

//        TODO put in array instead of LinkedList
        LinkedList<Point> queryPoints = loadQueryPoints();

        long startQueryTime = System.currentTimeMillis();

        for (Point queryPoint : queryPoints){
//            System.out.println(queryPoint.getX() + " " + queryPoint.getY());
            nnQuery(quadTree, queryPoint.getX(), queryPoint.getY(), sc);
//            System.out.println();
//            System.out.println();
        }

        long estimatedQueryResponseTime = System.currentTimeMillis() - startQueryTime;
        long estimatedTotalTime = System.currentTimeMillis() - startTotalTime;
        System.out.println("Total time: " + estimatedTotalTime + " miliseconds");
        System.out.println("Query response time: " + estimatedQueryResponseTime + " miliseconds");
        System.out.println("Building QuadTree time: " + estimatedQuadTreeBuildingTime + " miliseconds");

        saveTimeMeasurementToFile(estimatedQueryResponseTime, estimatedTotalTime, estimatedQuadTreeBuildingTime, quadTree);
        System.out.println("Datapoints accessed: " + datapointsAccessed);
    }

    private void saveTimeMeasurementToFile(long estimatedQueryResponseTime, long estimatedTime, long estimatedQuadTreeBuildingTime, QuadTreeArray quadTree) {


        StringBuilder timeOutput = new StringBuilder();
        timeOutput.append(DATA_TYPE).append(",")
                .append(DATASET).append(",")
                .append(QUERY_POINTS).append(",")
                .append(this.nodeSize).append(",")
                .append(k).append(",")
                .append(RECREATE_QUADTREE).append(",")
                .append(estimatedTime).append(",")
                .append(estimatedQueryResponseTime).append(",")
                .append(estimatedQuadTreeBuildingTime).append(",")
                .append(datapointsAccessed).append(",")
                .append(filesAccessed).append(",")
                .append(identificationTime).append(",")
                .append(fileAccessingTime).append(",")
                .append(knnCalcualtionTime).append(",")
                .append(verificationTime).append(",")
                .append(resultsFetching).append(",")
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
                        .append("TotalTime").append(",")
                        .append("TotalQueryResponseTime").append(",")
                        .append("QuadTreeBuildingTime").append(",")
                        .append("datapoints accessed").append(",")
                        .append("files accessed").append(",")
                        .append("identification time").append(",")
                        .append("file accessing time").append(",")
                        .append("knn computation time").append(",")
                        .append("verification time").append(",")
                        .append("results fetching").append(",")
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


    private QuadTreeArray recreateQuadTreeFromSumamryFile(QuadTreeArray quadTree, String summaryFile) {
        quadTree.createQuadTreeFromSummaryFile(FILE_PATH_SUMMARY + summaryFile);

        return quadTree;
    }

    private static JavaSparkContext sparkConfigSetUp() {
        //        disable the log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf().setAppName("knn_queries_spark_core").setMaster("local");
        return new JavaSparkContext(conf);
    }

    private LinkedList<Point> loadQueryPoints() {

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

    private QuadTreeArray buildQuadTree(QuadTreeArray quadTree) {

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

    private void cleanQuadTreeDataDirectory() {
        try {
            FileUtils.cleanDirectory(new File(FILE_PATH_QUADTREE_DATA));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void nnQuery(QuadTreeArray quadTree, double x, double y, JavaSparkContext sc) {
        long startInnerQRT = System.currentTimeMillis();

        long startIdentification = System.nanoTime();
        NodeArray mainPartition = quadTree.findPariton(x, y);
        long identificationTime = System.nanoTime() - startIdentification;



        datapointsAccessed += mainPartition.getSize();
        filesAccessed++;


        long startFileAccessing = System.nanoTime();
        JavaRDD<String> pointsfromFileString = sc.textFile( mainPartition.getFileName());

        JavaRDD<Tuple2<Double,Double>> pointsfromFileTuple = pointsfromFileString.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.apply(Double.parseDouble(arr[0]), Double.parseDouble(arr[1]));
        });
        long fileAccessingTime = System.nanoTime() - startFileAccessing;



        long startKnnCalcualtion = System.nanoTime();
        JavaRDD<Tuple3<Double,Double,Double>> distancesToPoints = pointsfromFileTuple.map(s -> {
            double distance = Math.sqrt(Math.pow(x - s._1, 2) + Math.pow(y - s._2, 2));

            return new Tuple3<Double, Double, Double>(s._1, s._2, distance);
        });

        distancesToPoints = distancesToPoints.sortBy(z -> z._3(), true, 1);
        long knnCalcualtionTime = System.nanoTime() - startKnnCalcualtion;




        long startVerification = System.nanoTime();
        Circle circle = findFurthestNeighbourCircle(x,y,distancesToPoints);
        LinkedList<NodeArray> partitons = quadTree.findPartitions(quadTree.getRootNodeArray(), circle, mainPartition);

        for ( NodeArray partition : partitons){
            datapointsAccessed += partition.getSize();
            filesAccessed++;
            distancesToPoints = distancesToPoints.union(findNearestNeighbours(partition,x,y,sc));
        }

        distancesToPoints = distancesToPoints.sortBy(z -> z._3(), true, 1);
        long verificationTime = System.nanoTime() - startVerification;


        long startResultsFetching = System.nanoTime();
//        System.out.println();
        for (Tuple3 s : distancesToPoints.take(k)){
//            System.out.println(s);
        }
        long resultsFetching = System.nanoTime() - startResultsFetching;


        this.identificationTime += identificationTime/1000000;
        this.fileAccessingTime += fileAccessingTime/1000000;
        this.knnCalcualtionTime += knnCalcualtionTime/1000000;
        this.verificationTime += verificationTime/1000000;
        this.resultsFetching += resultsFetching/1000000;
    }


    private Circle findFurthestNeighbourCircle(double x_coordinate, double y_coordinate, JavaRDD<Tuple3<Double,Double,Double>>  nearestNeighbours) {
        double distance = 0;
        for (Tuple3 s : nearestNeighbours.take(k)){
            distance = (double) s._3();
        }
        return new Circle(x_coordinate, y_coordinate, distance);
    }

    private JavaRDD<Tuple3<Double, Double, Double>> findNearestNeighbours(NodeArray partition, double x_coordinate, double y_coordinate, JavaSparkContext sc) {

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


