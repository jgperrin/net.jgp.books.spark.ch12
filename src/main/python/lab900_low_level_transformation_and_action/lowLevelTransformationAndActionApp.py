"""
 Low level transformations and actions.

 Those methods and classes are described in detailed in
 appendix T of the book.

 @author rambabu.posa
"""
import os
import logging
from pyspark.sql import (SparkSession, functions as F)

def get_absolute_file_path(path, filename):
    current_dir = os.path.dirname(__file__)
    relative_path = "{}{}".format(path, filename)
    absolute_file_path = os.path.join(current_dir, relative_path)
    return absolute_file_path

def reduceByKey(v1, v2):
    return v1 + ", " + v2

def print_rows(row):
    logging.warning(f"{row['Geography']} had {row['real2010']} inhabitants in 2010.")

def main(spark):
    path = '../../../../data/census/'
    filename = "PEP_2017_PEPANNRES.csv"
    absolute_file_path = get_absolute_file_path(path, filename)

    # Data ingestion and preparation
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(absolute_file_path)

    df = df.withColumnRenamed("GEO.id", "id") \
        .withColumnRenamed("GEO.id2", "id2") \
        .withColumnRenamed("GEO.display-label", "Geography") \
        .withColumnRenamed("rescen42010", "real2010") \
        .drop("resbase42010") \
        .withColumnRenamed("respop72010", "estimate2010") \
        .withColumnRenamed("respop72011", "estimate2011") \
        .withColumnRenamed("respop72012", "estimate2012") \
        .withColumnRenamed("respop72013", "estimate2013") \
        .withColumnRenamed("respop72014", "estimate2014") \
        .withColumnRenamed("respop72015", "estimate2015") \
        .withColumnRenamed("respop72016", "estimate2016") \
        .withColumnRenamed("respop72017", "estimate2017")

    df.printSchema()
    df.show(5)

    # Master dataframe
    countyStateDf = df \
        .withColumn("State", F.split(F.col("Geography"), ", ").getItem(1)) \
        .withColumn("County", F.split(F.col("Geography"), ", ").getItem(0))

    countyStateDf.show(5)

    # ---------------
    # Transformations
    # ---------------
    # map
    logging.warning("map()")
    dfMap = df.select("id2").withColumn("id2", F.substring(F.col("id2"), 3, 2))
    dfMap.show(5)

    # filter
    logging.warning("filter()")
    dfFilter = df.filter(F.col("real2010") < 30000)
    dfFilter.show(5)

    # flatMap
    logging.warning("flatMap()")
    county_state_df = df.select("Geography") \
        .withColumn("Geography", F.explode(F.split(F.col("Geography"), ",")))
    county_state_df.show(5)

    # mapPartitions
    # TODO: work on it to improve output
    logging.warning("mapPartitions()")
    dfPartitioned = df.repartition(10)
    dfMapPartitions = dfPartitioned.select("Geography") \
        .withColumn("Geography", F.explode(F.split(F.col("Geography"), ",")))

    logging.warning(f"Input dataframe has {df.count()} records")
    logging.warning(f"Result dataframe has {dfMapPartitions.distinct().count()} records")
    dfMapPartitions.show(5)

    # groupBy
    # Equal to groupByKey() in Spark(Scala) and Spark(Java) API
    logging.warning("groupBy()")
    group_by_key_df = df.select("id").withColumn("state", F.substring(F.col("id"), 10, 2)).groupBy("state")
    group_by_key_df.count().show(5)

    # dropDuplicates
    logging.warning("dropDuplicates()")
    stateDf = countyStateDf.dropDuplicates(["State"])
    stateDf.show(5)
    logging.warning(f"stateDf has {stateDf.count()} rows.")

    # agg
    logging.warning("agg()")
    countCountDf = countyStateDf.agg(F.count("County"))
    countCountDf.show(5)

    # ---------------------------------------------------------------------------
    # Actions
    # ---------------------------------------------------------------------------
    # reduce
    logging.warning("reduce()")
    county_state_df.show()
    list_of_county_state = [row.Geography for row in county_state_df.take(10)]
    separator = ', '
    logging.warning(separator.join(list_of_county_state))

    # foreach
    logging.warning("foreach()")
    df.sample(0.01).foreach(print_rows)


if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Low level transofrmation and actions") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()