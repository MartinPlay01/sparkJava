import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class ArrayToDataset {
  
  public void start() {
    SparkSession spark = new SparkSession.Builder()
      .appName("Array to Dataset<String>")
      .master("local")
      .getOrCreate();
    
    String[] stringsList = new String[]{"Banana", "Car", "Glass", "Banana", "Computer", "Car"};
    
    List<String> data = Arrays.asList(stringsList);
    
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    
    /*change dataset to dataframe*/
    Dataset<Row> df = ds.toDF();
  
    /*change dataframe to dataset*/
    ds = df.as(Encoders.STRING());
    
    ds.printSchema();
    ds.show();
    
    Dataset<Row> df2 = ds.groupBy("value").count();
    df2.show();
  }
  
}
