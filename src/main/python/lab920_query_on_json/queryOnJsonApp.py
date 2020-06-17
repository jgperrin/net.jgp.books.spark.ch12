"""
  Using JSONpath-like in SQL queries.

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
    path = '../../../../data/json/'
    filename = "store.json"
    absolute_file_path = get_absolute_file_path(path, filename)

    # Reads a JSON, stores it in a dataframe
    df = spark.read.format("json") \
        .option("multiline", True) \
        .load(absolute_file_path)

   # Explode the array
    df = df.withColumn("items", F.explode(F.col("store.book")))

    # Creates a view so I can use SQL
    df.createOrReplaceTempView("books")
    sqlQuery = "SELECT items.author FROM books WHERE items.category = 'reference'"

    authorsOfReferenceBookDf = spark.sql(sqlQuery)
    authorsOfReferenceBookDf.show(truncate=False)

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Query on a JSON doc") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()