import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.cleanandsure.mappers.LineMapper;

public class WordCount {
  
  
  public void start() {
    
    
    String boringWords = " ('a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by',\r\n" +
      "'for', 'if', 'in', 'into', 'is', 'it',\r\n" +
      "'no', 'not', 'of', 'on', 'or', 'such',\r\n" +
      "'that', 'the', 'their', 'then', 'there', 'these',\r\n" +
      "'they', 'this', 'to', 'was', 'will', 'with', 'he', 'she'," +
      "'your', 'you', 'I', "
      + " 'i','[',']', '[]', 'his', 'him', 'our', 'we') ";
    
    SparkSession spark = SparkSession.builder()
      .appName("Unstructured text to flatmap")
      .master("local")
      .getOrCreate();
    
    String filename = "src/main/resources/shakespeare.txt";

//    Every line is take as a record
    Dataset<Row> df = spark.read().format("text")
      .load(filename);
  
/*    df.printSchema();
    df.show(10, 90);*/
    
    Dataset<String> wordsDS = df.flatMap(new LineMapper(), Encoders.STRING());
    
    
    Dataset<Row> df2 = wordsDS.toDF()
      .groupBy("value").count();
    
    df2 = df2.orderBy(df2.col("count").desc())
      .filter("lower(value) NOT IN " + boringWords);
    df2.show(20);
    
  }
}
