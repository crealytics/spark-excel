# Spark Excel Library

A library for querying Excel files with Apache Spark, for Spark SQL and DataFrames.

[![Build Status](https://travis-ci.org/crealytics/spark-excel.svg?branch=master)](https://travis-ci.org/crealytics/spark-excel)

## Requirements

This library requires Spark 1.4+

## Linking
You can link against this library in your program at the following coordinates:

### Scala 2.11
```
groupId: com.crealytics
artifactId: spark-excel_2.11
version: 0.9.1
```

## Using with Spark shell
This package can be added to  Spark using the `--packages` command line option.  For example, to include it when starting the spark shell:

### Spark compiled with Scala 2.11
```
$SPARK_HOME/bin/spark-shell --packages com.crealytics:spark-excel_2.11:0.9.1
```

## Features
This package allows querying Excel spreadsheets as [Spark DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html).

### Scala API
__Spark 2.0+:__


#### Create a DataFrame from an Excel file
```scala
import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.read
    .format("com.crealytics.spark.excel")
    .option("sheetName", "Daily") // Required
    .option("useHeader", "true") // Required
    .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
    .option("inferSchema", "false") // Optional, default: false
    .option("addColorColumns", "true") // Optional, default: false
    .option("startColumn", 0) // Optional, default: 0
    .option("endColumn", 99) // Optional, default: Int.MaxValue
    .option("timestampFormat", "hh:mm:ss:SSS") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
    .schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
    .load("Worktime.xlsx")
```

#### Write a DataFrame to an Excel file
```scala
df.write
  .format("com.crealytics.spark.excel")
  .option("sheetName", "Daily")
  .option("useHeader", "true")
  .option("timestampFormat", "HH:mm:ss") // Optional, default: yyyy-MM-dd HH:mm:ss.SSS
  .mode("overwrite")
  .save("Worktime2.xlsx")
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html).
To build a JAR file simply run `sbt assembly` from the project root.
The build configuration includes support for Scala 2.11.
