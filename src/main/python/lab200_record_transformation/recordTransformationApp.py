"""
 Transforming records.

 @author rambabu.posa
"""
from pyspark.sql import SparkSession

# Creates a session on a local master
spark = SparkSession.builder.appName("Record transformations") \
    .master("local[*]").getOrCreate()


