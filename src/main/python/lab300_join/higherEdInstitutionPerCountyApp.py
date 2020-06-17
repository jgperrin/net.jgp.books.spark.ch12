"""
  Performs a join between 3 datasets to build a list of higher education
  institutions per county.

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

def main(spark):
    path = '../../../../data/census/'
    filename = "PEP_2017_PEPANNRES.csv"
    absolute_file_path = get_absolute_file_path(path, filename)

    # Ingestion of the census data
    censusDf = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "cp1252") \
        .load(absolute_file_path)

    censusDf = censusDf.drop("GEO.id") \
        .drop("rescen42010") \
        .drop("resbase42010") \
        .drop("respop72010") \
        .drop("respop72011") \
        .drop("respop72012") \
        .drop("respop72013") \
        .drop("respop72014") \
        .drop("respop72015") \
        .drop("respop72016") \
        .withColumnRenamed("respop72017", "pop2017") \
        .withColumnRenamed("GEO.id2", "countyId") \
        .withColumnRenamed("GEO.display-label", "county")

    logging.warning("Census data")
    censusDf.sample(0.1).show(3, False)
    censusDf.printSchema()

    path = '../../../../data/dapip/'
    filename = "InstitutionCampus.csv"
    absolute_file_path = get_absolute_file_path(path, filename)

    # Higher education institution (and yes, there is an Arkansas College
    # of Barbering and Hair Design)
    higherEdDf = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(absolute_file_path)

    higherEdDf = higherEdDf \
     .filter("LocationType = 'Institution'") \
     .withColumn("addressElements", F.split(F.col("Address"), " "))

    higherEdDf = higherEdDf.withColumn("addressElementCount",
            F.size(F.col("addressElements")))

    higherEdDf = higherEdDf.withColumn("zip9",
            F.element_at(F.col("addressElements"), F.col("addressElementCount")))

    higherEdDf = higherEdDf.withColumn("splitZipCode",
            F.split(F.col("zip9"), "-"))

    higherEdDf = higherEdDf \
        .withColumn("zip", F.col("splitZipCode").getItem(0)) \
        .withColumnRenamed("LocationName", "location") \
        .drop("DapipId", "OpeId", "ParentName", "ParentDapipId",
              "LocationType", "Address", "GeneralPhone", "AdminName",
              "AdminPhone", "AdminEmail", "Fax", "UpdateDate", "zip9",
              "addressElements", "addressElementCount", "splitZipCode")

    logging.warning("Higher education institutions (DAPIP)")
    higherEdDf.sample(0.1).show(3, False)
    higherEdDf.printSchema()

    path = '../../../../data/hud/'
    filename = "COUNTY_ZIP_092018.csv"
    absolute_file_path = get_absolute_file_path(path, filename)

    # Zip to county
    countyZipDf = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(absolute_file_path)

    countyZipDf = countyZipDf \
    .drop("res_ratio", "bus_ratio", "oth_ratio", "tot_ratio")

    logging.warning("Counties / ZIP Codes (HUD)")
    countyZipDf.sample(0.1).show(3, False)
    countyZipDf.printSchema()

    institPerCountyJoinCondition = higherEdDf["zip"] == countyZipDf["zip"]

    # Institutions per county id
    institPerCountyDf = higherEdDf.join(countyZipDf, institPerCountyJoinCondition, "inner") \
        .drop(countyZipDf["zip"])

    logging.warning("Higher education institutions left-joined with HUD")
    institPerCountyDf.filter(F.col("zip") == 27517).show(20, False)
    institPerCountyDf.printSchema()

    # --------------------------
    # - "Temporary" drop columns
    # Note:
    # This block is not doing anything except illustrating that the drop()
    # method needs to be used carefully.
    # Dropping all zip columns
    logging.warning("Attempt to drop the zip column")
    institPerCountyDf.drop("zip") \
        .sample(0.1) \
        .show(3, False)

    # Dropping the zip column inherited from the higher ed dataframe
    logging.warning("Attempt to drop the zip column")
    institPerCountyDf.drop("zip") \
        .sample(0.1) \
        .show(3, False)
    # --------------------------

    institPerCountyCondition = institPerCountyDf["county"] == censusDf["countyId"]
    # Institutions per county name
    institPerCountyDf = institPerCountyDf.join(censusDf, institPerCountyCondition, "left") \
        .drop(censusDf["county"])

    # Final clean up
    institPerCountyDf = institPerCountyDf \
        .drop("zip", "county", "countyId") \
        .distinct()

    logging.warning("Higher education institutions in ZIP Code 27517 (NC)")
    institPerCountyDf.filter(F.col("zip") == 27517).show(20, False)

    logging.warning("Higher education institutions in ZIP Code 02138 (MA)")
    institPerCountyDf.filter(higherEdDf["zip"] == 2138).show(20, False)

    logging.warning("Institutions with improper counties")
    institPerCountyDf.filter("county is null").show(200, False)

    logging.warning("Final list")
    institPerCountyDf.show(200, False)
    logging.warning(f"The combined list has {institPerCountyDf.count()} elements.")

    # A little more
    # aggDf = institPerCountyDf.groupBy("county", "pop2017").count()
    # aggDf = aggDf.orderBy(aggDf["count"].desc())
    # aggDf.show(25, False)
    #
    # popDf = aggDf.filter("pop2017>30000") \
    #   .withColumn("institutionPer10k", F.expr("count*10000/pop2017"))
    #
    # popDf = popDf.orderBy(popDf["institutionPer10k"].desc())
    # popDf.show(25, False)

if __name__ == "__main__":
    # Creates a session on a local master
    spark = SparkSession.builder.appName("Join") \
        .master("local[*]").getOrCreate()

    # setting log level, update this as per your requirement
    spark.sparkContext.setLogLevel("warn")

    main(spark)
    spark.stop()