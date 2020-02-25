import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
public class Application {
  
  public static void main(String[] args) {
  
    SparkSession spark = SparkSession.builder()
      .appName("Combine two datasets")
      .master("local")
      .getOrCreate();
    
    Dataset<Row> martinDf = buildMartinSparkDataframe(spark);
    
    /*martinDf.printSchema();//now we can classify the variables*/
    martinDf.show(20, 90);
    /*Dataset<Row> philDf = buildPhilParkDataframe(spark);*/
  
/*      combineDataframes(philDf, martinDf);*/
  
  }
  
  public static Dataset<Row> buildMartinSparkDataframe(SparkSession spark){
  
    Dataset<Row> df = spark.read().format("json")
      .option("multiline", true)
      .load("src/main/resources/jsonformatterCoinOne.short.json");
    
    Dataset<Row> lala =df.withColumn("data", explode(col("data")))
      .withColumn("credit_card", col("status").getField("credit_count"))
      .withColumn("name", col("data").getField("name"))//getItem(0)for take first element of an array
      .withColumn("cmc_rank", col("data").getField("cmc_rank"))
      .withColumn("date_added", col("data").getField("date_added"))
      .withColumn("last_updated", col("data").getField("last_updated"))
      .withColumn("market_cap", col("data").getField("quote").getField("USD").getField("market_cap"))
      .withColumn("price", col("data").getField("quote").getField("USD").getField("price"))
      .withColumn("last_updated", col("data").getField("quote").getField("USD").getField("last_updated"))
      .withColumn("percent_change_1h", col("data").getField("quote").getField("USD").getField("percent_change_1h"))
      .withColumn("percent_change_24h", col("data").getField("quote").getField("USD").getField("percent_change_24h"))
      .withColumn("percent_change_7d", col("data").getField("quote").getField("USD").getField("percent_change_7d"))
      .withColumn("volume_24h", col("data").getField("quote").getField("USD").getField("volume_24h"))
      .drop("data").drop("status");

    return lala;
  }
}
