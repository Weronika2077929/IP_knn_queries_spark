import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.util.LinkedList;
import java.util.Scanner;

public class KnnQueriesSparkSQLTesting {
    private static String FILE_PATH = "C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/";
    private static String FILE_PATH_OUTPUT = "C:/Users/Wera/Documents/4thyear/IP/time_output_SparkSql.txt";
    private static String DATA_TYPE = "normal";
    private String DATASET;
    private String QUERY_POINTS;
    private String FILE_NAME_DATASET ;
    private String FILE_NAME_QUERY_POINTS;
    private int k;

    public KnnQueriesSparkSQLTesting(String DATASET, String QUERY_POINTS, int k) {
        this.DATASET = DATASET;
        this.QUERY_POINTS = QUERY_POINTS;
        this.k = k;
        this.FILE_NAME_DATASET = FILE_PATH + DATASET;
        this.FILE_NAME_QUERY_POINTS = FILE_PATH + QUERY_POINTS;

    }

    public static void main(String[] args) {

        SparkSession spark = sparkSetUp();

//        KnnQueriesSparkSQLTesting test = new KnnQueriesSparkSQLTesting("10000000", "100", 10);
//        test.run(spark);
//        KnnQueriesSparkSQLTesting test1 = new KnnQueriesSparkSQLTesting("10000000", "100", 100);
//        test1.run(spark);
//        KnnQueriesSparkSQLTesting test2 = new KnnQueriesSparkSQLTesting("10000000", "100", 1000);
//        test2.run(spark);

//        KnnQueriesSparkSQLTesting test3 = new KnnQueriesSparkSQLTesting("multivarTest.csv", "100", 10);
//        test3.run(spark);
//        KnnQueriesSparkSQLTesting test4 = new KnnQueriesSparkSQLTesting("multivarTest.csv", "100", 100);
//        test4.run(spark);
//        KnnQueriesSparkSQLTesting test5 = new KnnQueriesSparkSQLTesting("multivarTest.csv", "100", 1000);
//        test5.run(spark);

//        KnnQueriesSparkSQLTesting test6 = new KnnQueriesSparkSQLTesting("Gowalla_Sample.csv", "100_smallerRange.csv", 10);
//        test6.run(spark);
//        KnnQueriesSparkSQLTesting test7 = new KnnQueriesSparkSQLTesting("Gowalla_Sample.csv", "100_smallerRange.csv", 100);
//        test7.run(spark);
//        KnnQueriesSparkSQLTesting test8 = new KnnQueriesSparkSQLTesting("Gowalla_Sample.csv", "100_smallerRange.csv", 1000);
//        test8.run(spark);




    }

    private static SparkSession sparkSetUp(){
        //        disable the log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
//
        SparkSession spark = SparkSession
                .builder().master("local")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        return spark;
    }

    private void run(SparkSession spark) {

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

        long startTime = System.currentTimeMillis();

        Dataset<Row> df = spark.read().csv(FILE_NAME_DATASET);

        df.createOrReplaceTempView("points");

        long estimatedTime = System.currentTimeMillis() - startTime;

        for( Double[] queryPoint : queryPoints){
//            System.out.println(queryPoint[0] + " " + queryPoint[1]);

            long startTime2 = System.currentTimeMillis();
            Dataset<Row> sqlVar = spark.sql("SELECT * FROM points ORDER BY SQRT(POWER( "
                    + queryPoint[0]
                    + " - _c0 , 2) + POWER("
                    + queryPoint[1]
                    + " - _c1 , 2))");

            sqlVar.take(k);
            estimatedTime += System.currentTimeMillis() - startTime2;
//            sqlVar.show(k);
        }

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