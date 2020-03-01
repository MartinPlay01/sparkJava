import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {
  
  public static void main(String[] args) {
  
  
    SparkSession spark = SparkSession.builder()
      .appName("Learning spark SQL Dataframe API")
      .master("local")
      .getOrCreate();
  
    String filename1 = "src/main/resources/students.csv";
    String filename2 = "src/main/resources/grade_chart.csv";
  
    Dataset<Row> df1 = spark.read().format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(filename1);
  
    Dataset<Row> df2 = spark.read().format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .load(filename2);
  
    df1.join(df2, df1.col("GPA").equalTo(df2.col("GPA")))
      .select(df1.col("student_name"),
        df1.col("favorite_book_title"),
        df2.col("letter_grade")).show(5);
  
  }
}
