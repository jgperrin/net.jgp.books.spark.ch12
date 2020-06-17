"""
  Processing of invoices formatted using the schema.org format.

 @author rambabu.posa
"""
import os
from pyspark.sql import (SparkSession, functions as F)

def get_absolute_file_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = "{}{}".format(path, filename)
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path

def main(spark):
    path = '../../../../data/invoice/'
    filename = "good-invoice*.json"
    absolute_file_path = get_absolute_file_path(path, filename)

    # Reads a JSON, called countrytravelinfo.json, stores it in a dataframe
    invoices_df = spark.read.format("json") \
        .option("multiline", True) \
        .load(absolute_file_path)

    # Shows at most 3 rows from the dataframe
    invoices_df.show(3)
    invoices_df.printSchema()

    invoice_amount_df = invoices_df.select("totalPaymentDue.*")
    invoice_amount_df.show(5)
    invoice_amount_df.printSchema()

    elements_ordered_by_account_df = invoices_df.select(F.col("accountId"),
            F.explode(F.col("referencesOrder")).alias("order"))

    elements_ordered_by_account_df = elements_ordered_by_account_df \
        .withColumn("type", F.col("order.orderedItem.@type")) \
        .withColumn("description", F.col("order.orderedItem.description")) \
        .withColumn("name", F.col("order.orderedItem.name"))
    # TODO: failing to drop struct field
    #.drop("order")

    elements_ordered_by_account_df.show(10)
    elements_ordered_by_account_df.printSchema()

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Processing of invoices") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()