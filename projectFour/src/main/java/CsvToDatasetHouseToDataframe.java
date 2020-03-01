import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.cleanandsure.mappers.CoinsMapper;
import org.cleanandsure.pojos.Coins;

import static org.apache.spark.sql.functions.*;

public class CsvToDatasetHouseToDataframe {

  public void start(){
  
    SparkSession spark = new SparkSession.Builder()
      .appName("CSV to Dataframe to Dataset<House> and back")
      .master("local")
      .getOrCreate();
    
    String filename = "src/main/resources/archivocomma.csv";
    
    Dataset<Row> df = spark.read().format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .option("sep", ",")
      .load(filename);
  
    System.out.println("*** Coins ingested in a Dataframe: ");
    
    df.show(5, 90);
    df.printSchema();
    
    Dataset<Coins> coinsDS = df.map(new CoinsMapper(), Encoders.bean(Coins.class));
  
    System.out.println("*** Coins ingested in a Dataset: ");
    coinsDS.show(5, 90);
    coinsDS.printSchema();
    
/*    Dataset<Row> df2 = coinsDS.toDF();
    df2 = df2.withColumn("formatedDate", concat(col("date_added.year"),
      lit("_"),col("date_added.month"),
      lit("_"), col("date_added.date")));
    
    df2.show(5, 90);
    df2.printSchema();*/
  }

}
