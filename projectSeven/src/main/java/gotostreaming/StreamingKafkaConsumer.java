package gotostreaming;


import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class StreamingKafkaConsumer {
  
  public static void main(String[] args) throws StreamingQueryException {
    SparkSession spark = SparkSession.builder()
      .appName("Streaming kafka consumer")
      .master("local")
      .getOrCreate();


//    Kafka consumer
    Dataset<Row> messageDf = spark.readStream()
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
      .option("subscribe", "test")
      .load()
      .selectExpr("CAST(value AS STRING)");// lines.selectExpr("CAST key AS STRING", "CAST value AS STRING") for key value

//    messageDf.show() -> can't do this when streaming!
    Dataset<String> words = messageDf
      .as(Encoders.STRING())
      .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
    
    Dataset<Row> wordCount = words.groupBy("value").count();
  
  
    StreamingQuery query = wordCount.writeStream()
      .outputMode("complete")
      .format("console")
      .start();
    
    query.awaitTermination();
    
  }
}
