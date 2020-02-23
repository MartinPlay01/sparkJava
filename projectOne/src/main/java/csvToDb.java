import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

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
    
    df = df.withColumn("fullname",
      concat(df.col("first_name"), lit(", "), df.col("last_name")))
      .filter(df.col("comment").rlike("\\d+"))
      .orderBy(df.col("last_name").asc());
    
    df.show(5);
    
    
    String dbConnectionUrl = "jdbc:postgresql://localhost/course_data";
    Properties prop = new Properties();
    prop.setProperty("driver", "org.postgresql.Driver");
    prop.setProperty("user", "lala");
    prop.setProperty("password", "lala");
    
    
        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "projectOne", prop);
  }
  
}
