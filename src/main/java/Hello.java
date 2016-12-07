///**
// * Created by Wera on 2016-11-13.
// */
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

import java.util.function.BiFunction;

import static spark.Spark.*;

public class Hello {
    public static void main(String[] args) {

        get("/hello", (req, res) -> "Hello world");

//        number of nn queries to find
        int k = 5;

//        the coordinates of the main point of reference
        double x_coordinate = 0;
        double y_coordinate = 0;

//        disable the log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
//
        SparkSession spark = SparkSession
                .builder().master("local")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> df = spark.read().csv("C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/10.csv");

//        System.out.println("min " + df.filter(min("_c0")).show(); );
//        df.groupBy("_c0").show();

//        Column c0 = df.col("_c0");
//
//        for(Row row: df.collectAsList()) {
//            Double x = Double.parseDouble(row.getString(0));
//            Double y = Double.parseDouble(row.getString(1));
//            System.out.println("x: " + x + ", y: " +y);
//        }

        SQLContext sqlContext = new SQLContext(spark);

        sqlContext.udf().register("myFunction", (String x, String y) ->
                    Math.sqrt(Math.pow(x_coordinate - Double.parseDouble(x), 2) + Math.pow(y_coordinate - Double.parseDouble(y), 2)
                ), DataTypes.DoubleType);

        Column distance = callUDF("myFunction", col("_c0"), col("_c1"));
        df = df.withColumn("distance", distance).orderBy("distance");

        df.show(k);
    }
}