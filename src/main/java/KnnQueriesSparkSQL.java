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

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.function.BiFunction;

import static spark.Spark.*;

public class KnnQueriesSparkSQL {
    private static String FILE_PATH = "C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/";
    private static String FILE_PATH_OUTPUT = "C:/Users/Wera/Documents/4thyear/IP/time_output_SparkSql.txt";
    private static String DATA_TYPE = "normal";
    private static String DATASET = "10000000";
    private static String QUERY_POINTS = "10";
    private static String FILE_NAME_DATASET = FILE_PATH + DATASET;
    private static String FILE_NAME_QUERY_POINTS = FILE_PATH + QUERY_POINTS;
    private static int k = 10;


    public static void main(String[] args) {

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

        long startTime1 = System.currentTimeMillis();

        Dataset<Row> df = spark.read().csv(FILE_NAME_DATASET);

        df.createOrReplaceTempView("points");

        long estimatedTime = System.currentTimeMillis() - startTime1;

        for( Double[] queryPoint : queryPoints){
            System.out.println(queryPoint[0] + " " + queryPoint[1]);

            long startTime2 = System.currentTimeMillis();
             Dataset<Row> sqlVar = spark.sql("SELECT * FROM points ORDER BY SQRT(POWER( "
                     + queryPoint[0]
                     + " - _c0 , 2) + POWER("
                     + queryPoint[1]
                     + " - _c1 , 2))");
             estimatedTime += System.currentTimeMillis() - startTime2;
            sqlVar.show(k);
        }

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