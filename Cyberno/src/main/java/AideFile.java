import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class AideFile {
  
  public static void sinkCSVFile(Dataset<Row> ds, String path){
    ds.coalesce(1).write().option("header", true).csv(path);
  }


}
