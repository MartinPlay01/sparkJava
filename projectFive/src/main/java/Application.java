import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

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
    
    Dataset<Row> filteredDf = df1.join(df2, df1.col("GPA").equalTo(df2.col("GPA")))
      .filter(df2.col("gpa").gt(3.0).and(df2.col("GPA").lt(4.5))
        .or(df2.col("gpa").equalTo(1.0)))
      .select("student_name",
        "favorite_book_title",
        "letter_grade");
    
  }
}
