# Spark Excel Library

A library for querying Excel files with Apache Spark, for Spark SQL and DataFrames.

[![Build Status](https://github.com/crealytics/spark-excel/workflows/CI/badge.svg)](https://github.com/crealytics/spark-excel/actions)
[![Sonatype Nexus](https://img.shields.io/nexus/s/https/oss.sonatype.org/com.crealytics/spark-excel_2.12.svg)](https://oss.sonatype.org/content/repositories/public/com/crealytics/spark-excel_2.12/)
[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/crealytics/spark-excel)

## Co-maintainers wanted
Due to personal and professional constraints, the development of this library has been rather slow.
If you find value in this library, please consider stepping up as a co-maintainer by leaving a comment [here](https://github.com/crealytics/spark-excel/issues/191).
Help is very welcome e.g. in the following areas:

* Additional features
* Code improvements and reviews
* Bug analysis and fixing
* Documentation improvements
* Build / test infrastructure

## Requirements

This library requires Spark 2.0+

## Linking
You can link against this library in your program at the following coordinates:

### Scala 2.12
```
groupId: com.crealytics
artifactId: spark-excel_2.12
version: 0.13.1
```

### Scala 2.11
```
groupId: com.crealytics
artifactId: spark-excel_2.11
version: 0.13.1
```

## Using with Spark shell
This package can be added to  Spark using the `--packages` command line option.  For example, to include it when starting the spark shell:

### Spark compiled with Scala 2.12
```
$SPARK_HOME/bin/spark-shell --packages com.crealytics:spark-excel_2.12:0.13.1
```

### Spark compiled with Scala 2.11
```
$SPARK_HOME/bin/spark-shell --packages com.crealytics:spark-excel_2.11:0.13.1
```

## Features
This package allows querying Excel spreadsheets as [Spark DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html).

### Scala API
__Spark 2.0+:__


#### Create a DataFrame from an Excel file

```scala
import org.apache.spark.sql._

val spark: SparkSession = ???
val df = spark.read
    .format("com.crealytics.spark.excel")
    .option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
    .option("header", "true") // Required
    .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
    .option("setErrorCellsToFallbackValues", "true") // Optional, default: false, where errors will be converted to null. If true, any ERROR cell values (e.g. #N/A) will be converted to the zero values of the column's data type.
    .option("usePlainNumberFormat", "false") // Optional, default: false, If true, format the cells without rounding and scientific notations
    .option("inferSchema", "false") // Optional, default: false
    .option("addColorColumns", "true") // Optional, default: false
    .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
    .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
    .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
    .option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs
    .schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
    .load("Worktime.xlsx")
```

For convenience, there is an implicit that wraps the `DataFrameReader` returned by `spark.read`
and provides a `.excel` method which accepts all possible options and provides default values:

```scala
import org.apache.spark.sql._
import com.crealytics.spark.excel._

val spark: SparkSession = ???
val df = spark.read.excel(
    header = true,  // Required
    dataAddress = "'My Sheet'!B3:C35", // Optional, default: "A1"
    treatEmptyValuesAsNulls = false,  // Optional, default: true
    setErrorCellsToFallbackValues = false, // Optional, default: false, where errors will be converted to null. If true, any ERROR cell values (e.g. #N/A) will be converted to the zero values of the column's data type.
    usePlainNumberFormat = false,  // Optional, default: false. If true, format the cells without rounding and scientific notations
    inferSchema = false,  // Optional, default: false
    addColorColumns = true,  // Optional, default: false
    timestampFormat = "MM-dd-yyyy HH:mm:ss",  // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
    maxRowsInMemory = 20,  // Optional, default None. If set, uses a streaming reader which can help with big files
    excerptSize = 10,  // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
    workbookPassword = "pass"  // Optional, default None. Requires unlimited strength JCE for older JVMs
).schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
 .load("Worktime.xlsx")
```

If the sheet name is unavailable, it is possible to pass in an index:

```scala
val df = spark.read.excel(
  header = true,
  dataAddress = "0!B3:C35"
).load("Worktime.xlsx")
```

or to read in the names dynamically:

```scala
val sheetNames = WorkbookReader( Map("path" -> "Worktime.xlsx")
                               , spark.sparkContext.hadoopConfiguration
                               ).sheetNames
val df = spark.read.excel(
  header = true,
  dataAddress = sheetNames(0)
)
```

#### Create a DataFrame from an Excel file using custom schema
```scala
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val peopleSchema = StructType(Array(
    StructField("Name", StringType, nullable = false),
    StructField("Age", DoubleType, nullable = false),
    StructField("Occupation", StringType, nullable = false),
    StructField("Date of birth", StringType, nullable = false)))

val spark: SparkSession = ???
val df = spark.read
    .format("com.crealytics.spark.excel")
    .option("sheetName", "Info")
    .option("header", "true")
    .schema(peopleSchema)
    .load("People.xlsx")
```

#### Write a DataFrame to an Excel file
```scala
import org.apache.spark.sql._

val df: DataFrame = ???
df.write
  .format("com.crealytics.spark.excel")
  .option("dataAddress", "'My Sheet'!B3:C35")
  .option("header", "true")
  .option("dateFormat", "yy-mmm-d") // Optional, default: yy-m-d h:mm
  .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
  .mode("append") // Optional, default: overwrite.
  .save("Worktime2.xlsx")
```

#### Data Addresses
As you can see in the examples above,
the location of data to read or write can be specified with the `dataAddress` option.
Currently the following address styles are supported:

* `B3`: Start cell of the data.
  Reading will return all rows below and all columns to the right.
  Writing will start here and use as many columns and rows as required.
* `B3:F35`: Cell range of data.
  Reading will return only rows and columns in the specified range.
  Writing will start in the first cell (`B3` in this example) and use only the specified columns and rows.
  If there are more rows or columns in the DataFrame to write, they will be truncated.
  Make sure this is what you want.
* `'My Sheet'!B3:F35`: Same as above, but with a specific sheet.
* `MyTable[#All]`: Table of data.
  Reading will return all rows and columns in this table.
  Writing will only write within the current range of the table.
  No growing of the table will be performed. PRs to change this are welcome.


## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html).
To build a JAR file simply run `sbt assembly` from the project root.
The build configuration includes support for Scala 2.12 and 2.11.
