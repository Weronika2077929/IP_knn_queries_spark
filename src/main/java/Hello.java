///**
// * Created by Wera on 2016-11-13.
// */
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Hello {
    public static void main(String[] args) {

//        get("/hello", (req, res) -> "Hello world");

//        disable the log messages
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession spark = SparkSession
                .builder().master("local")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();


//        Dataset<Row> df = spark.read().json("C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/people.json");
        Dataset<Row> df = spark.read().text("C:/Users/Wera/Documents/4thyear/IP/Java_Spark_Project/src/main/resources/100");
        df.show();
//        df.select("name").show();
        System.out.println(df.count());

    }
}