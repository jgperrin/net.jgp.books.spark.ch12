The examples in this repository are support to the **[Spark in Action, 2nd edition](http://jgp.net/sia)** book by Jean-Georges Perrin and published by Manning. Find out more about the book on [Manning's website](http://jgp.net/sia).

# Spark in Action, 2nd edition – Java, Python, and Scala code for chapter 12

Welcome to Spark in Action 2nd edition, chapter 12. This chapter is about **data transformation**.

This code is designed to work with Apache Spark v3.1.2.

Datasets can be downloaded from:
* [United States Census Data](https://factfinder.census.gov/bkmk/table/1.0/en/PEP/2017/PEPANNRES/0100000US.05000.004|0400000US01.05000|0400000US02.05000|0400000US04.05000|0400000US05.05000|0400000US06.05000|0400000US08.05000|0400000US09.05000|0400000US10.05000|0400000US11.05000|0400000US12.05000|0400000US13.05000|0400000US15.05000|0400000US16.05000|0400000US17.05000|0400000US18.05000|0400000US19.05000|0400000US20.05000|0400000US21.05000|0400000US22.05000|0400000US23.05000|0400000US24.05000|0400000US25.05000|0400000US26.05000|0400000US27.05000|0400000US28.05000|0400000US29.05000|0400000US30.05000|0400000US31.05000|0400000US32.05000|0400000US33.05000|0400000US34.05000|0400000US35.05000|0400000US36.05000|0400000US37.05000|0400000US38.05000|0400000US39.05000|0400000US40.05000|0400000US41.05000|0400000US42.05000|0400000US44.05000|0400000US45.05000|0400000US46.05000|0400000US47.05000|0400000US48.05000|0400000US49.05000|0400000US50.05000|0400000US51.05000|0400000US53.05000|0400000US54.05000|0400000US55.05000|0400000US56.05000)

## Labs

Each chapter has one or more labs. Labs are examples used for teaching in the [book](https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp). You are encouraged to take ownership of the code and modify it, experiment with it, hence the use of the term **lab**.

### Lab \#200

The `RecordTransformationApp` application does the following:

 1. It acquires a session (a `SparkSession`).
 1. It asks Spark to load (ingest) a dataset in CSV format.
 1. Spark stores the contents in a dataframe, then transforms the records.

### Lab \# 300

TBD

### Lab \# 301

TBD

### Lab \# 302

This lab is part of the continuous improvement on this project. It is not described in the book and illustrates the use of adaptive query execution (AQE) that is introduced in Apache Spark v3.

### Lab \# 900

TBD

### Lab \# 920

TBD

### Lab \# 930

TBD

### Lab \# 940

Lab demoing the use of all possible joins.

### Lab \# 941

Lab demoing the use of all possible joins with non-matching types.

### Lab \# 990

TBD

## Running the lab in Java

For information on running the Java lab, see chapter 1 in [Spark in Action, 2nd edition](http://jgp.net/sia).

## Running the lab using PySpark

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - 'Spark in production: installation and a few tips').

1. Clone this project

```
git clone https://github.com/jgperrin/net.jgp.books.spark.ch12
```

2. Go to the lab in the Python directory

```
cd net.jgp.books.spark.ch12/src/main/python/lab200_record_transformation/
```

3. Execute the following spark-submit command to create a jar file to our this application

 ```
spark-submit recordTransformationApp.py
 ```

## Running the lab in Scala

Prerequisites:

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - "Spark in production: installation and a few tips"). 

1. Clone this project

    git clone https://github.com/jgperrin/net.jgp.books.spark.ch12

2. cd net.jgp.books.spark.ch12

3. Package application using sbt command

   ```
     sbt clean assembly
   ```

4. Run Spark/Scala application using spark-submit command as shown below:

   ```
   spark-submit --class net.jgp.books.spark.ch12.lab200_record_transformation.RecordTransformationScalaApp target/scala-2.12/SparkInAction2-Chapter12-assembly-1.0.0.jar  
   ```

## News

 1. [2020-06-13] Updated the `pom.xml` to support Apache Spark v3.1.2. 
 1. [2020-06-13] As we celebrate the first anniversary of Spark in Action, 2nd edition is the best-rated Apache Spark book on [Amazon](https://amzn.to/2TPnmOv). 

## Notes

 1. [Java] Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10 (published in April 2019).
 1. [Scala, Python] As of MEAP v14, we have introduced Scala and Python examples (published in October 2019).
 1. The master branch contains the last version of the code running against the latest supported version of Apache Spark. Look in specifics branches for specific versions.

---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://fb.com/SparkInAction/) or in [Manning's community site](https://forums.manning.com/forums/spark-in-action-second-edition?a_aid=jgp).
