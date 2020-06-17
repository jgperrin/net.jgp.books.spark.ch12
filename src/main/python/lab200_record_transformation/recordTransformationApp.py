"""
 Transforming records.

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
    path = '../../../../data/census/'
    filename = "PEP_2017_PEPANNRES.csv"
    absolute_file_path = get_absolute_file_path(path, filename)

    # Ingestion of the census data
    intermediate_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(absolute_file_path)

    # Renaming and dropping the columns we do not need
    intermediate_df = intermediate_df.drop("GEO.id") \
        .withColumnRenamed("GEO.id2", "id") \
        .withColumnRenamed("GEO.display-label", "label") \
        .withColumnRenamed("rescen42010", "real2010") \
        .drop("resbase42010") \
        .withColumnRenamed("respop72010", "est2010") \
        .withColumnRenamed("respop72011", "est2011") \
        .withColumnRenamed("respop72012", "est2012") \
        .withColumnRenamed("respop72013", "est2013") \
        .withColumnRenamed("respop72014", "est2014") \
        .withColumnRenamed("respop72015", "est2015") \
        .withColumnRenamed("respop72016", "est2016") \
        .withColumnRenamed("respop72017", "est2017")

    intermediate_df.printSchema()
    intermediate_df.show(5)

    # Creates the additional columns
    intermediate_df = intermediate_df \
    .withColumn("countyState", F.split(F.col("label"), ", ")) \
    .withColumn("stateId", F.expr("int(id/1000)")) \
    .withColumn("countyId", F.expr("id%1000"))

    intermediate_df.printSchema()
    intermediate_df.sample(.01).show(5, False)
    
    intermediate_df = intermediate_df \
        .withColumn("state", F.col("countyState").getItem(1)) \
        .withColumn("county", F.col("countyState").getItem(0)) \
        .drop("countyState")
    
    intermediate_df.printSchema()
    intermediate_df.sample(.01).show(5, False)

    # I could split the column in one operation if I wanted:
    # countyStateDf: Dataset[Row] = intermediateDf
    #  .withColumn("state", F.split(F.col("label"), ", ").getItem(1))
    #  .withColumn("county", F.split(F.col("label"), ", ").getItem(0))

    # Performs some statistics on the intermediate dataframe
    statDf = intermediate_df \
        .withColumn("diff", F.expr("est2010-real2010")) \
        .withColumn("growth", F.expr("est2017-est2010")) \
        .drop("id") \
        .drop("label") \
        .drop("real2010") \
        .drop("est2010") \
        .drop("est2011") \
        .drop("est2012") \
        .drop("est2013") \
        .drop("est2014") \
        .drop("est2015") \
        .drop("est2016") \
        .drop("est2017")

    statDf.printSchema()
    statDf.sample(.01).show(5, False)

    # Extras: see how you can sort!
    # statDf = statDf.sort(F.col("growth").desc())
    # print("Top 5 counties with the most growth:")
    # statDf.show(5, False)
    #
    # statDf = statDf.sort(F.col("growth"))
    # print("Top 5 counties with the most loss:")
    # statDf.show(5, False)

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Record transformations") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()
