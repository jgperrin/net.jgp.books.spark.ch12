package net.jgp.books.spark.ch12.lab920_query_on_json

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * Using JSONpath-like in SQL queries.
 *
 * @author rambabu.posa
 */
object QueryOnJsonScalaApp {

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
      .appName("Query on a JSON doc")
      .master("local[*]")
      .getOrCreate

    // Reads a JSON, stores it in a dataframe
    val df = spark.read
      .format("json")
      .option("multiline", true)
      .load("data/json/store.json")

    // Explode the array
    val df2 = df.withColumn("items", F.explode(F.col("store.book")))

    // Creates a view so I can use SQL
    df2.createOrReplaceTempView("books")
    val sqlQuery = "SELECT items.author FROM books WHERE items.category = 'reference'"
    val authorsOfReferenceBookDf = spark.sql(sqlQuery)
    authorsOfReferenceBookDf.show(false)

    spark.stop
  }

}
