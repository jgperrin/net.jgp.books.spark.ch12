package net.jgp.books.spark.ch12.lab900_low_level_transformation_and_action

import java.util.{Arrays, Iterator}

import org.apache.spark.api.java.function._
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{Encoders, Row, SparkSession, functions => F}

/**
 * Low level transformations and actions.
 *
 * Those methods and classes are described in detailed in appendix T of the
 * book.
 *
 * @author rambabu.posa
 */
object LowLevelTransformationAndActionScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    val spark: SparkSession = SparkSession.builder
      .appName("Low level transofrmation and actions")
      .master("local[*]")
      .getOrCreate

    // Data ingestion and preparation
    var df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/census/PEP_2017_PEPANNRES.csv")

    df = df.withColumnRenamed("GEO.id", "id")
      .withColumnRenamed("GEO.id2", "id2")
      .withColumnRenamed("GEO.display-label", "Geography")
      .withColumnRenamed("rescen42010", "real2010")
      .drop("resbase42010")
      .withColumnRenamed("respop72010", "estimate2010")
      .withColumnRenamed("respop72011", "estimate2011")
      .withColumnRenamed("respop72012", "estimate2012")
      .withColumnRenamed("respop72013", "estimate2013")
      .withColumnRenamed("respop72014", "estimate2014")
      .withColumnRenamed("respop72015", "estimate2015")
      .withColumnRenamed("respop72016", "estimate2016")
      .withColumnRenamed("respop72017", "estimate2017")

    df.printSchema()
    df.show(5)

    // Master dataframe
    val countyStateDf = df
      .withColumn("State", F.split(F.col("Geography"), ", ").getItem(1))
      .withColumn("County", F.split(F.col("Geography"), ", ").getItem(0))

    countyStateDf.show(5)

    // ---------------
    // Transformations
    // ---------------
    // map
    println("map()")
    val dfMap = df.map(new CountyFipsExtractorUsingMap, Encoders.STRING)
    dfMap.show(5)

    // filter
    println("filter()")
    val dfFilter = df.filter(new SmallCountiesUsingFilter)
    dfFilter.show(5)

    // flatMap
    println("flatMap()")
    val countyStateDs = df.flatMap(new CountyStateExtractorUsingFlatMap, Encoders.STRING)
    countyStateDs.show(5)

    // mapPartitions
    println("mapPartitions()")
    val dfPartitioned = df.repartition(10)
    val dfMapPartitions = dfPartitioned.mapPartitions(new FirstCountyAndStateOfPartitionUsingMapPartitions, Encoders.STRING)
    println("Input dataframe has " + df.count + " records")
    println("Result dataframe has " + dfMapPartitions.count + " records")
    dfMapPartitions.show(5)

    // groupByKey
    println("groupByKey()")
    val groupByKeyDs = df.groupByKey(new StateFipsExtractorUsingMap, Encoders.STRING)
    groupByKeyDs.count.show(5)

    // dropDuplicates
    println("dropDuplicates()")
    val stateDf = countyStateDf.dropDuplicates("State")
    stateDf.show(5)
    println("stateDf has " + stateDf.count + " rows.")

    // agg
    println("agg()")
    val countCountDf = countyStateDf.agg(count("County"))
    countCountDf.show(5)

    // ---------------
    // Actions
    // ---------------
    // reduce
    println("reduce()")
    val listOfCountyStateDs = countyStateDs.reduce(new CountyStateConcatenatorUsingReduce)
    println(listOfCountyStateDs)

    // foreach
    println("foreach()")
    df.foreach(new DisplayCountyPopulationForeach)

    spark.stop
  }

  /**
   * Concatenates the counties and states
   *
   * @author rambabu.posa
   */
  @SerialVersionUID(12859L)
  final private class CountyStateConcatenatorUsingReduce extends ReduceFunction[String] {
    @throws[Exception]
    override def call(v1: String, v2: String): String = v1 + ", " + v2
  }

  /**
   * Returns a substring of the values in the id2 column.
   *
   * @author rambabu.posa
   */
  @SerialVersionUID(26547L)
  final private class CountyFipsExtractorUsingMap extends MapFunction[Row, String] {
    @throws[Exception]
    override def call(r: Row): String = {
      val s = r.getAs("id2").toString.substring(2)
      s
    }
  }

  /**
   * Extracts the state id from each row.
   *
   * @author rambabu.posa
   */
  @SerialVersionUID(26572L)
  final private class StateFipsExtractorUsingMap extends MapFunction[Row, String] {
    @throws[Exception]
    override def call(r: Row): String = {
      val id = r.getAs("id").toString
      val state = id.substring(9, 11)
      state
    }
  }

  /**
   * Filters on counties with less than 30,000 inhabitants.
   *
   * @author rambabu.posa
   */
  @SerialVersionUID(17392L)
  final private class SmallCountiesUsingFilter extends FilterFunction[Row] {
    @throws[Exception]
    override def call(r: Row): Boolean = {
      if (r.getInt(4) < 30000) return true
      false
    }
  }

  /**
   *
   * @author rambabu.posa
   *
   */
  @SerialVersionUID(63784L)
  class CountyStateExtractorUsingFlatMap extends FlatMapFunction[Row, String] {
    @throws[Exception]
    override def call(r: Row): Iterator[String] = {
      val s: Array[String] = r.getAs("Geography").toString.split(", ")
      Arrays.stream(s).iterator
    }
  }

  /**
   *
   * @author rambabu.posa
   *
   */
  @SerialVersionUID(-62694L)
  class FirstCountyAndStateOfPartitionUsingMapPartitions extends MapPartitionsFunction[Row, String] {
    @throws[Exception]
    override def call(input: Iterator[Row]): Iterator[String] = {
      val r = input.next
      val s = r.getAs("Geography").toString.split(", ")
      Arrays.stream(s).iterator
    }
  }

  /**
   * Displays the population of a county for the first 10 counties in the
   * dataset.
   *
   * @author rambabu.posa
   */
  @SerialVersionUID(14738L)
  final private class DisplayCountyPopulationForeach extends ForeachFunction[Row] {
    private var count = 0

    @throws[Exception]
    override def call(r: Row): Unit = {
      if (count < 10) println(r.getAs("Geography").toString + " had " + r.getAs("real2010").toString + " inhabitants in 2010.")
      count += 1
    }
  }

}
