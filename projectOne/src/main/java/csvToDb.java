import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class csvToDb {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("csv to DB")
                .master("local")
                .getOrCreate();

        //get data
        Dataset<Row> df = spark.read().format("csv")
                .option("header", true)
                .load("src/main/resources/name_and_comments.txt");

        df.show(5);
    }

}
