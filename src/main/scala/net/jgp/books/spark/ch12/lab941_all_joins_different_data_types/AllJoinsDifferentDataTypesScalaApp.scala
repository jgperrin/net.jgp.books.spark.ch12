package net.jgp.books.spark.ch12.lab941_all_joins_different_data_types

import java.util.ArrayList

import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{Row, RowFactory, SparkSession}

/**
 * All joins in a single app, inspired by
 * https://stackoverflow.com/questions/45990633/what-are-the-various-join-types-in-spark.
 *
 * Used in Spark in Action 2e, http://jgp.net/sia
 *
 * @author rambabu.posa
 */
object AllJoinsDifferentDataTypesScalaApp {

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
      .appName("Processing of invoices")
      .master("local[*]")
      .getOrCreate

    val schemaLeft = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("id", DataTypes.IntegerType, false),
      DataTypes.createStructField("value", DataTypes.StringType, false)))

    val schemaRight = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("idx", DataTypes.StringType, false),
      DataTypes.createStructField("value", DataTypes.StringType, false)))

    var rows = new ArrayList[Row]
    rows.add(RowFactory.create(int2Integer(1), "Value 1"))
    rows.add(RowFactory.create(int2Integer(2), "Value 2"))
    rows.add(RowFactory.create(int2Integer(3), "Value 3"))
    rows.add(RowFactory.create(int2Integer(4), "Value 4"))

    val dfLeft = spark.createDataFrame(rows, schemaLeft)
    dfLeft.show()

    rows = new ArrayList[Row]
    rows.add(RowFactory.create("3", "Value 3"))
    rows.add(RowFactory.create("4", "Value 4"))
    rows.add(RowFactory.create("4", "Value 4_1"))
    rows.add(RowFactory.create("5", "Value 5"))
    rows.add(RowFactory.create("6", "Value 6"))

    val dfRight = spark.createDataFrame(rows, schemaRight)
    dfRight.show()

    val joinTypes = List(
      "inner", // v2.0.0. default
      "outer", // v2.0.0
      "full", // v2.1.1
      "full_outer",
      "left",
      "left_outer",
      "right",
      "right_outer",
      "left_semi", // v2.0.0, was leftsemi before v2.1.1
      "left_anti",
      "cross" // v2.2.0
    )

    for (joinType <- joinTypes) {
      println(joinType.toUpperCase + " JOIN")
      val df = dfLeft.join(dfRight, dfLeft.col("id") === dfRight.col("idx"), joinType)
      df.orderBy(dfLeft.col("id")).show()
    }

    println("CROSS JOIN (without a column")
    val df = dfLeft.crossJoin(dfRight)
    df.orderBy(dfLeft.col("id")).show()

    spark.stop

  }

}
