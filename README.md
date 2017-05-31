# Spark Excel Library

A library for querying Excel files with Apache Spark, for Spark SQL and DataFrames.

[![Build Status](https://travis-ci.org/crealytics/spark-excel.svg?branch=master)](https://travis-ci.org/crealytics/spark-excel)

## Requirements

This library requires Spark 1.4+

## Linking
You can link against this library in your program at the following coordinates:

### Scala 2.10
```
groupId: com.crealytics
artifactId: spark-excel_2.10
version: 0.8.3
```
### Scala 2.11
```
groupId: com.crealytics
artifactId: spark-excel_2.11
version: 0.8.3
```

## Using with Spark shell
This package can be added to  Spark using the `--packages` command line option.  For example, to include it when starting the spark shell:

### Spark compiled with Scala 2.11
```
$SPARK_HOME/bin/spark-shell --packages com.crealytics:spark-excel_2.11:0.8.3
```

### Spark compiled with Scala 2.10
```
$SPARK_HOME/bin/spark-shell --packages com.crealytics:spark-excel_2.10:0.8.3
```

## Features
This package allows querying Excel spreadsheets as [Spark DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html).

### Scala API
__Spark 1.4+:__


Create a DataFrame from an Excel file:
```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.read
    .format("com.crealytics.spark.excel")
    .option("location", "Worktime.xlsx")
    .option("sheetName", "Daily")
    .option("useHeader", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .option("addColorColumns", "true")
    .load()
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html).
To build a JAR file simply run `sbt assembly` from the project root.
The build configuration includes support for both Scala 2.10 and 2.11.
