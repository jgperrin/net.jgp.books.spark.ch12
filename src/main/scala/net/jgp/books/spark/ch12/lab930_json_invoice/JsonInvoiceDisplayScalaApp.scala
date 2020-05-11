package net.jgp.books.spark.ch12.lab930_json_invoice

import org.apache.spark.sql.{SparkSession, functions => F}

/**
 * Processing of invoices formatted using the schema.org format.
 *
 * @author rambabu.posa
 */
object JsonInvoiceDisplayScalaApp {

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

    // Reads a JSON, called countrytravelinfo.json, stores it in a dataframe
    val invoicesDf = spark.read
      .format("json")
      .option("multiline", true)
      .load("data/invoice/good-invoice*.json")

    // Shows at most 3 rows from the dataframe
    invoicesDf.show(3)
    invoicesDf.printSchema()

    val invoiceAmountDf = invoicesDf.select("totalPaymentDue.*")
    invoiceAmountDf.show(5)
    invoiceAmountDf.printSchema()

    var elementsOrderedByAccountDf = invoicesDf
      .select(F.col("accountId"),
        F.explode(F.col("referencesOrder")).as("order"))

    elementsOrderedByAccountDf = elementsOrderedByAccountDf
      .withColumn("type", F.col("order.orderedItem.@type"))
      .withColumn("description", F.col("order.orderedItem.description"))
      .withColumn("name", F.col("order.orderedItem.name"))
      .drop(F.col("order"))
      //.drop("order")

    elementsOrderedByAccountDf.show(10)
    elementsOrderedByAccountDf.printSchema()

    spark.stop
  }

}
