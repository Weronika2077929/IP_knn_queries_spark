import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

public class MyLauncher {
    public static void main(String[] args) throws Exception {
        SparkAppHandle handle = new SparkLauncher()
                .setAppResource("C:\\Users\\Wera\\Documents\\4th year GU\\IP NN queries\\Java_Spark_Project\\out\\artifacts\\Sparki_jar")
                .setMainClass("Hello")
                .setSparkHome("C:\\Spark")
                .setVerbose(true)
                .setMaster("local")
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .startApplication();

        // Use handle API to monitor / control application.
    }
}