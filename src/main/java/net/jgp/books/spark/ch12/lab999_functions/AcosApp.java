package net.jgp.books.spark.ch12.lab999_functions;

import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * acos function: inverse cosine of a value in radians
 * 
 * @author jgp
 */
public class AcosApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    AcosApp app = new AcosApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("acos function")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .load("data/functions/trigo.csv");

    df = df.withColumn("acos", abs(col("angle_rad")));

    df.show();
  }
}
