import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

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


        Dataset<Row> df2 = df.withColumn("fullname", concat(df.col("first_name"), lit(", ") , df.col("last_name")));

        df2 = df2.filter(df.col("comment").rlike("\\d+")).orderBy(df.col("last_name").asc());

        df2.show(5);
    }

}
