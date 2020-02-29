import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
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

//    ds = ds.map(new StringMapper(), Encoders.STRING());
    
    /*Using lambda functions and not work with class StringMapper minus code*/
    ds = ds.map((MapFunction<String, String>) row -> "word: " + row, Encoders.STRING());
    
    String stringValue = ds.reduce(new StringReducer());//Not encoder because return a single String
    
    System.out.println("String value: "+ stringValue);
    
  }
  
  
/*  static class StringMapper implements MapFunction<String, String>, Serializable {
  
  
    @Override
    public String call(String value) throws Exception {
      return "word: "+ value;
    }
  }*/
  
  static class StringReducer implements ReduceFunction<String>, Serializable {
    
    @Override
    public String call(String s, String t1) throws Exception {
      return s + t1;
    }
  }
  
  
}
