package gosparkwithjava;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;

public class Application {
  
  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder()
      .appName("Reddit Analyze comments")
      .getOrCreate();
//      .master("local")//remove this line for execute in distributed mode


//    add uri for s3

//    String reddit_file = "s3://myspark-course-bucket/RC_2011-12";
//    String reddit_file2 = "/home/mizquierdo/Documentos/cursos/sparkIntellij/data/RC_2011-12";
    String reddit_file = "s3://myspark-course-bucket/reddit_2007-01-small";
    
    Dataset<Row> redditDf = spark.read().format("json")
      .option("inferSchema", "true")
      .option("header", true)
      .load(reddit_file);
    
    redditDf = redditDf.select("body");
    
    Dataset<String> wordsDs = redditDf.flatMap((FlatMapFunction<Row, String>)
        r -> Arrays.asList(r.toString().replace("\n", "")
          .replace("\r", "")
          .trim().toLowerCase()
          .split(" ")).iterator()
      , Encoders.STRING());
    
    
    Dataset<Row> wordsDf = wordsDs.toDF();
    
    Dataset<Row> wordStop = spark
      .createDataset(Arrays.asList(WordUtils.stopWords)
        , Encoders.STRING()).toDF();
    
//    wordsDf = wordsDf.except(wordStop); // <--- this remove duplicates from the end dataframe
    wordsDf = wordsDf.join(wordStop, wordsDf.col("value").equalTo(wordStop.col("value")), "leftanti");
    
    wordsDf = wordsDf.groupBy("value").count();
    
    wordsDf.orderBy(desc("count"));
    
    
    wordsDf.printSchema();
    
    wordsDf.show(3, 90);
  }
}
