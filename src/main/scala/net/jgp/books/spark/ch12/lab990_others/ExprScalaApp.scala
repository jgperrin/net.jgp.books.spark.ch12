package net.jgp.books.spark.ch12.lab990_others

import java.util.ArrayList
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession, functions => F}

/**
 * Use of expr() Spark API.
 *
 * @author rambabu.posa
 */
object ExprScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    /**
     * The processing code.
     */
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("expr()")
      .master("local[*]")
      .getOrCreate

    val schema: StructType = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("title", DataTypes.StringType, false),
      DataTypes.createStructField("start", DataTypes.IntegerType, false),
      DataTypes.createStructField("end", DataTypes.IntegerType, false)))

    val rows = new ArrayList[Row]
    rows.add(RowFactory.create("bla", 10, 30))
    var df: Dataset[Row] = spark.createDataFrame(rows, schema)
    df.show()

    df = df.withColumn("time_spent", F.expr("end - start"))
      .drop("start")
      .drop("end")

    df.show()

    spark.stop
  }

}
