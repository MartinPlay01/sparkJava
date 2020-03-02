import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class CustomerAndProducts {
  
  public static void main(String[] args) {
    
    SparkSession spark = SparkSession.builder()
      .appName("Learning spark SQL Dataframe API")
      .master("local")
      .getOrCreate();
    
    String customer_file = "src/main/resources/customers.csv";
    String product_file = "src/main/resources/products.csv";
    String purchases_file = "src/main/resources/purchases.csv";
    
    Dataset<Row> customerDf = spark.read().format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(customer_file);
    
    Dataset<Row> productDf = spark.read().format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(product_file);
    
    Dataset<Row> purchaseDf = spark.read().format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(purchases_file);
    
    Dataset<Row> joinnedDate = customerDf.join(purchaseDf, customerDf.col("customer_id")
      .equalTo(purchaseDf.col("customer_id")))
      .join(productDf, purchaseDf.col("product_id").equalTo(productDf.col("product_id")))
      .drop("favorite_website").drop(purchaseDf.col("customer_id"))
      .drop(purchaseDf.col("product_id")).drop("product_id");
    
    joinnedDate.groupBy("first_name", "product_name").agg(
      count("product_name").as("number_of_purchases"),
      max("product_price").as("most_exp_purchases"),
      sum("product_price").as("total_spent")
    ).show();
    
  }
  
}
