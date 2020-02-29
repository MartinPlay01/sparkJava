import org.apache.spark.Partition;
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
    
    /*first execution of method*/
    Dataset<Row> martinDf = buildMartinSparkDataframe(spark);
    
    /*second execution of method*/
    Dataset<Row> philDf = buildPhilParkDataframe(spark);
    philDf.show(5);
    
    combineDataframes(philDf, martinDf);
  
  }
  
  /*when we use methods as a union, we need a minimum of 2 partitions*/
  private static void combineDataframes(Dataset<Row> philDf, Dataset<Row> martinDf) {
    /*Match by column name using the unionByName method
      if we use the union() method, it matches the columns based on order.*/
    Dataset<Row> df = philDf.unionByName(martinDf);
    df.show(200);
    df.printSchema();
    System.out.println("we have " + df.count() + " records.");
    
    Partition[] partitions = df.rdd().partitions();
    System.out.println("total number of partitions "+ partitions.length);
  
  }
  
  
  public static Dataset<Row> buildMartinSparkDataframe(SparkSession spark){
  
    Dataset<Row> df = spark.read().format("json")
      .option("multiline", true)
      .load("src/main/resources/jsonformatterCoinOne.json");
    
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
      .withColumn("slug", col("data").getField("slug"))
      .withColumn("symbol", col("data").getField("symbol"))
      .withColumn("total_supply", col("data").getField("total_supply"))
      .drop("data").drop("status");
  
    lala = lala.filter(col("symbol").rlike("^(BTC|ETH|XRP)"));
  
    return lala;
  }
  
  private static Dataset<Row> buildPhilParkDataframe(SparkSession spark) {
    Dataset<Row> df2 = spark.read().format("json")
      .option("multiline", true)
      .load("src/main/resources/dataset2.json");
    
    /*df = df.filter(lower(df.col("PPR_USE")).like("%park%"));*/
    /*df = df.filter("lower(PPR_USE) like '%park%' ");*/
    Dataset<Row> lolo = df2.withColumn("data", explode(col("data")))
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
      .withColumn("slug", col("data").getField("slug"))
      .withColumn("symbol", col("data").getField("symbol"))
      .withColumn("total_supply", col("data").getField("total_supply"))
      .drop("data").drop("status");
    
    lolo = lolo.filter(col("symbol").rlike("^(BTC|ETH|XRP)"));
//    lolo = lolo.filter("lower(symbol) in 'btc, eth, xrp'");
    
    return lolo;
  }
}
