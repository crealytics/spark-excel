# Spark Excel Library

A library for querying Excel files with Apache Spark, for Spark SQL and DataFrames.

[![Build Status](https://github.com/crealytics/spark-excel/workflows/CI/badge.svg)](https://github.com/crealytics/spark-excel/actions)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.crealytics/spark-excel_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.crealytics/spark-excel_2.12)


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

This library requires Spark 2.0+.

List of spark versions, those are automatically tested:
```
spark: ["2.4.1", "2.4.7", "2.4.8", "3.0.1", "3.0.3", "3.1.1", "3.1.2", "3.2.4", "3.3.2", "3.4.1"]
```
For more detail, please refer to project CI: [ci.yml](https://github.com/crealytics/spark-excel/blob/main/.github/workflows/ci.yml#L10)

## Linking
You can link against this library in your program at the following coordinates:

### Scala 2.12
```
groupId: com.crealytics
artifactId: spark-excel_2.12
version: <spark-version>_0.18.0
```

### Scala 2.11
```
groupId: com.crealytics
artifactId: spark-excel_2.11
version: <spark-version>_0.13.7
```

## Using with Spark shell
This package can be added to  Spark using the `--packages` command line option.  For example, to include it when starting the spark shell:

### Spark compiled with Scala 2.12
```
$SPARK_HOME/bin/spark-shell --packages com.crealytics:spark-excel_2.12:<spark-version>_0.18.0
```

### Spark compiled with Scala 2.11
```
$SPARK_HOME/bin/spark-shell --packages com.crealytics:spark-excel_2.11:<spark-version>_0.13.7
```

## Features
* This package allows querying Excel spreadsheets as [Spark DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html).
* From spark-excel [0.14.0](https://github.com/crealytics/spark-excel/releases/tag/v0.14.0) (August 24, 2021), there are two implementation of spark-excel
    * Original Spark-Excel with Spark data source API 1.0
    * Spark-Excel V2 with data source API V2.0+, which supports loading from multiple files, corrupted record handling and some improvement on handling data types.
      See below for further details

To use V2 implementation, just change your .format from `.format("com.crealytics.spark.excel")` to `.format("excel")`.
See [below](#excel-api-based-on-datasourcev2) for some details

See the [changelog](CHANGELOG.md) for latest features, fixes etc.

### Scala API
__Spark 2.0+:__


#### Create a DataFrame from an Excel file

```scala
import org.apache.spark.sql._

val spark: SparkSession = ???
val df = spark.read
    .format("com.crealytics.spark.excel") // Or .format("excel") for V2 implementation
    .option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
    .option("header", "true") // Required
    .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
    .option("setErrorCellsToFallbackValues", "true") // Optional, default: false, where errors will be converted to null. If true, any ERROR cell values (e.g. #N/A) will be converted to the zero values of the column's data type.
    .option("usePlainNumberFormat", "false") // Optional, default: false, If true, format the cells without rounding and scientific notations
    .option("inferSchema", "false") // Optional, default: false
    .option("addColorColumns", "true") // Optional, default: false
    .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
    .option("dateFormat", "yyyyMMdd") // Optional, default: yyyy-MM-dd
    .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files (will fail if used with xls format files)
    .option("maxByteArraySize", 2147483647) // Optional, default None. See https://poi.apache.org/apidocs/5.0/org/apache/poi/util/IOUtils.html#setByteArrayMaxOverride-int-
    .option("tempFileThreshold", 10000000) // Optional, default None. Number of bytes at which a zip entry is regarded as too large for holding in memory and the data is put in a temp file instead
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
    maxRowsInMemory = 20,  // Optional, default None. If set, uses a streaming reader which can help with big files (will fail if used with xls format files)
    maxByteArraySize = 2147483647,  // Optional, default None. See https://poi.apache.org/apidocs/5.0/org/apache/poi/util/IOUtils.html#setByteArrayMaxOverride-int-
    tempFileThreshold = 10000000, // Optional, default None. Number of bytes at which a zip entry is regarded as too large for holding in memory and the data is put in a temp file instead
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
import com.crealytics.spark.excel.WorkbookReader
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
    .format("com.crealytics.spark.excel") // Or .format("excel") for V2 implementation
    .option("dataAddress", "'Info'!A1")
    .option("header", "true")
    .schema(peopleSchema)
    .load("People.xlsx")
```

#### Write a DataFrame to an Excel file
```scala
import org.apache.spark.sql._

val df: DataFrame = ???
df.write
  .format("com.crealytics.spark.excel") // Or .format("excel") for V2 implementation
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

### Excel API based on DataSourceV2
The V2 API offers you several improvements when it comes to file and folder handling.
and works in a very similar way than data sources like csv and parquet.

To use V2 implementation, just change your .format from `.format("com.crealytics.spark.excel")` to `.format("excel")`

The big difference is the fact that you provide a path to read / write data from/to and not
an individual single file only:

```scala
dataFrame.write
        .format("excel")
        .save("some/path")
```

```scala
spark.read
        .format("excel")
        // ... insert excel read specific options you need
        .load("some/path")
```


Because folders are supported you can read/write from/to a "partitioned" folder structure, just
the same way as csv or parquet. Note that writing partitioned structures is only
available for spark >=3.0.1

````scala
dataFrame.write
        .partitionBy("col1")
        .format("excel")
        .save("some/path")
````

Need some more examples? Check out the [test cases](src/test/scala/com/crealytics/spark/excel/v2/DataFrameWriterApiComplianceSuite.scala)
or have a look at our wiki

## Building From Source
This library is built with [Mill](https://github.com/com-lihaoyi/mill).
To build a JAR file simply run e.g. `mill spark-excel[2.13.10,3.3.1].assembly` from the project root, where `2.13.10` is the Scala version and `3.3.1` the Spark version.
To list all available combinations of Scala and Spark, run `mill resolve spark-excel[__]`.

## Acknowledgements

This project was originally developed at [crealytics](https://crealytics.com), an award-winning full-funnel digital marketing agency with over 15 years of experience crafting omnichannel media strategies for leading B2C and B2B businesses.
We are grateful for their support in the initial development and open-sourcing of this library.

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=crealytics/spark-excel&type=Date)](https://star-history.com/#crealytics/spark-excel&Date)

