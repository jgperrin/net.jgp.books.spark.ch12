package net.jgp.books.spark.ch12.lab990_others

import java.util.{ArrayList, Random}

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, RowFactory, SparkSession, functions => F}

/**
 * Use of from_unixtime() and unix_timestamp().
 *
 * @author rambabu.posa
 */
object EpochTimestampConversionScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   * @throws InterruptedException
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
      DataTypes.createStructField("event", DataTypes.IntegerType, false),
      DataTypes.createStructField("original_ts", DataTypes.StringType, false)))

    // Building a df with a sequence of chronological timestamps
    val rows = new ArrayList[Row]
    var now: Long = System.currentTimeMillis / 1000

    for (i <- 0.until(1000)) {
      rows.add(RowFactory.create(i, String.valueOf(now)))
      now += new Random().nextInt(3) + 1
    }

    var df = spark.createDataFrame(rows, schema)
    df.show()
    df.printSchema()

    // Turning the timestamps to Timestamp datatype
    df = df.withColumn("date",
      F.from_unixtime(F.col("original_ts")).cast(DataTypes.TimestampType))
    df.show()
    df.printSchema()

    // Turning back the timestamps to epoch
    df = df.withColumn("epoch",
      F.unix_timestamp(F.col("date")))
    df.show()
    df.printSchema()

    // Collecting the result and printing ou
    import scala.collection.JavaConversions._
    for (row <- df.collectAsList) {
      printf("[%d] : %s (%s)\n", row.getInt(0), row.getAs("epoch"), row.getAs("date"))
    }

    spark.stop
  }

}
