import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalog.Function;
import org.apache.spark.sql.types.DataTypes;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.function.BiFunction;

import static spark.Spark.*;

public class KnnQueriesSparkSQL {
    private static String FILE_PATH = "C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/";
    private static String FILE_PATH_OUTPUT = "C:/Users/Wera/Documents/4thyear/IP/QuadTreeData/";
    private static String FILE_NAME_DATASET = FILE_PATH + "1000000";
    private static String FILE_NAME_QUERY_POINTS = FILE_PATH + "10";

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
//        get("/hello", (req, res) -> "Hello world");

//        number of nn queries to find
        int k = 5;

//        the coordinates of the main point of reference
        double x_coordinate = 0;
        double y_coordinate = 0;


        LinkedList<Double[]> queryPoints = new LinkedList<>();
        try {
            Scanner in = new Scanner(new FileReader(FILE_NAME_QUERY_POINTS));
            while(in.hasNext()) {
                String[] data = in.nextLine().split(",");
                Double[] a = {Double.parseDouble(data[0]),Double.parseDouble(data[1])};
                queryPoints.add(a);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

//        disable the log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
//
        SparkSession spark = SparkSession
                .builder().master("local")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> df = spark.read().csv(FILE_NAME_DATASET);

        df.createOrReplaceTempView("points");

//        Dataset<Row> sqlVarriable = spark.sql("SELECT * FROM points ORDER BY POWER( " + x_coordinate + " + _c0 , 2) + POWER(" + y_coordinate + " + _c1 , 2) LIMIT " + k);
//        sqlVarriable.show();

        for( Double[] queryPoint : queryPoints){
            System.out.println(queryPoint[0] + " " + queryPoint[1]);
             Dataset<Row> sqlVar = spark.sql("SELECT * FROM points ORDER BY SQRT(POWER( "
                     + queryPoint[0]
                     + " - _c0 , 2) + POWER("
                     + queryPoint[1]
                     + " - _c1 , 2))");

            sqlVar.show(k);
//            sqlVar.rdd().saveAsTextFile(FILE_PATH_OUTPUT + "sparkSQL.txt");
//            sqlVar.write().save(FILE_PATH_OUTPUT + "sparkSQL.txt");

        }

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println(estimatedTime + " miliseconds");
    }
}