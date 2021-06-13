"""
  E X P E R I M E N T A L
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
    census_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "cp1252") \
        .load(absolute_file_path)

    census_df = census_df.drop("GEO.id") \
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
    census_df.sample(0.1).show(3, False)
    census_df.printSchema()

    path = '../../../../data/dapip/'
    filename = "InstitutionCampus.csv"
    absolute_file_path = get_absolute_file_path(path, filename)
    
    # Higher education institution (and yes, there is an Arkansas College
    # of Barbering and Hair Design)
    higher_ed_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(absolute_file_path)

    higher_ed_df = higher_ed_df \
        .filter("LocationType = 'Institution'") \
        .withColumn("addressElements", F.split(F.col("Address"), " "))
        
    higher_ed_df = higher_ed_df.withColumn("addressElementCount",
        F.size(F.col("addressElements")))
    
    higher_ed_df = higher_ed_df.withColumn("zip9",
        F.element_at(F.col("addressElements"), F.col("addressElementCount")))
    
    higher_ed_df = higher_ed_df.withColumn("splitZipCode",
        F.split(F.col("zip9"), "-"))

    higher_ed_df = higher_ed_df \
        .withColumn("zip", F.col("splitZipCode")[0]) \
        .withColumnRenamed("LocationName", "location") \
        .drop("DapipId") \
        .drop("OpeId") \
        .drop("ParentName") \
        .drop("ParentDapipId") \
        .drop("LocationType") \
        .drop("Address") \
        .drop("GeneralPhone") \
        .drop("AdminName") \
        .drop("AdminPhone") \
        .drop("AdminEmail") \
        .drop("Fax") \
        .drop("UpdateDate") \
        .drop("zip9") \
        .drop("addressElements") \
        .drop("addressElementCount") \
        .drop("splitZipCode") \
        .alias("highered")

    logging.warning("Higher education institutions (DAPIP)")
    higher_ed_df.sample(0.1).show(3, False)
    higher_ed_df.printSchema()

    path = '../../../../data/hud/'
    filename = "COUNTY_ZIP_092018.csv"
    absolute_file_path = get_absolute_file_path(path, filename)

    # Zip to county
    county_zip_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(absolute_file_path)

    county_zip_df = county_zip_df \
        .drop("res_ratio") \
        .drop("bus_ratio") \
        .drop("oth_ratio") \
        .drop("tot_ratio") \
        .alias("hud")

    logging.warning("Counties / ZIP Codes (HUD)")
    county_zip_df.sample(0.1) \
        .show(3, False)
    
    county_zip_df.printSchema()

    # Institutions per county id
    instit_per_county_df = higher_ed_df.join(county_zip_df,
            higher_ed_df["zip"] == county_zip_df["zip"], "inner")

    logging.warning("Higher education institutions left-joined with HUD")
    instit_per_county_df.filter(higher_ed_df["zip"] == 27517) \
        .show(20, False)

    instit_per_county_df.printSchema()
    
    # Institutions per county name
    instit_per_county_df = instit_per_county_df.join(census_df,
        instit_per_county_df["county"] == census_df["countyId"], "left")

    logging.warning("Higher education institutions and county id with census")
    instit_per_county_df.filter(higher_ed_df["zip"] == 27517) \
        .show(20, False)
    
    instit_per_county_df.filter(higher_ed_df["zip"] == 2138) \
        .show(20, False)

    # Final clean up
    instit_per_county_df = instit_per_county_df.drop("highered.zip") \
        .drop("hud.county") \
        .drop("countyId") \
        .distinct()

    logging.warning("Final list")
    instit_per_county_df.show(200, False)

    logging.warning("The combined list has {} elements.".format(instit_per_county_df.count()))

    # A little more
    # aggDf = instit_per_county_df.groupBy("county", "pop2017").count()
    # aggDf = aggDf.orderBy(aggDf["count"].desc())
    # aggDf.show(25, False)
    #
    # popDf = aggDf \
    #   .filter("pop2017>30000") \
    #   .withColumn("institutionPer10k", F.expr("count*10000/pop2017"))
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