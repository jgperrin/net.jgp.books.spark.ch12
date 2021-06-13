package net.jgp.books.spark.ch12.lab302_join_aqe;

import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Performs a join between 3 datasets to build a list of higher education
 * institutions per county.
 * 
 * This example illustrates how to use adaptive query execution (AQE), a
 * feature added in Spark v3.
 * 
 * @author jgp
 */
public class HigherEdInstitutionPerCountyUsingAqeApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    HigherEdInstitutionPerCountyUsingAqeApp app =
        new HigherEdInstitutionPerCountyUsingAqeApp();
    boolean useAqe = false;
    app.start(useAqe);
    app.start(useAqe);
    app.start(useAqe);
    app.start(useAqe);
    app.start(useAqe);
  }

  /**
   * The processing code.
   */
  private void start(boolean useAqe) {
    // Creation of the session
    SparkSession spark = SparkSession.builder()
        .appName("Join using AQE")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", useAqe)
        .getOrCreate();

    // Ingestion of the census data
    Dataset<Row> censusDf = spark
        .read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "cp1252")
        .load("data/census/PEP_2017_PEPANNRES.csv");
    censusDf = censusDf
        .drop("GEO.id")
        .drop("rescen42010")
        .drop("resbase42010")
        .drop("respop72010")
        .drop("respop72011")
        .drop("respop72012")
        .drop("respop72013")
        .drop("respop72014")
        .drop("respop72015")
        .drop("respop72016")
        .withColumnRenamed("respop72017", "pop2017")
        .withColumnRenamed("GEO.id2", "countyId")
        .withColumnRenamed("GEO.display-label", "county");

    // Higher education institution (and yes, there is an Arkansas College
    // of Barbering and Hair Design)
    Dataset<Row> higherEdDf = spark
        .read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/dapip/InstitutionCampus.csv");
    higherEdDf = higherEdDf
        .filter("LocationType = 'Institution'")
        .withColumn(
            "addressElements",
            split(higherEdDf.col("Address"), " "));
    higherEdDf = higherEdDf
        .withColumn(
            "addressElementCount",
            size(higherEdDf.col("addressElements")));
    higherEdDf = higherEdDf
        .withColumn(
            "zip9",
            element_at(
                higherEdDf.col("addressElements"),
                higherEdDf.col("addressElementCount")));
    higherEdDf = higherEdDf
        .withColumn(
            "splitZipCode",
            split(higherEdDf.col("zip9"), "-"));
    higherEdDf = higherEdDf
        .withColumn("zip", higherEdDf.col("splitZipCode").getItem(0))
        .withColumnRenamed("LocationName", "location")
        .drop("DapipId")
        .drop("OpeId")
        .drop("ParentName")
        .drop("ParentDapipId")
        .drop("LocationType")
        .drop("Address")
        .drop("GeneralPhone")
        .drop("AdminName")
        .drop("AdminPhone")
        .drop("AdminEmail")
        .drop("Fax")
        .drop("UpdateDate")
        .drop("zip9")
        .drop("addressElements")
        .drop("addressElementCount")
        .drop("splitZipCode");

    // Zip to county
    Dataset<Row> countyZipDf = spark
        .read()
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/hud/COUNTY_ZIP_092018.csv");
    countyZipDf = countyZipDf
        .drop("res_ratio")
        .drop("bus_ratio")
        .drop("oth_ratio")
        .drop("tot_ratio");

    long t0 = System.currentTimeMillis();

    // Institutions per county id
    Dataset<Row> institPerCountyDf = higherEdDf.join(
        countyZipDf,
        higherEdDf.col("zip").equalTo(countyZipDf.col("zip")),
        "inner");

    // Institutions per county name
    institPerCountyDf = institPerCountyDf.join(
        censusDf,
        institPerCountyDf.col("county").equalTo(censusDf.col("countyId")),
        "left");

    // Action
    institPerCountyDf = institPerCountyDf.cache();
    institPerCountyDf.collect();

    long t1 = System.currentTimeMillis();
    System.out.println("AQE is " + useAqe +
        ", join operations took: " + (t1 - t0) + " ms.");

    spark.stop();
  }
}
