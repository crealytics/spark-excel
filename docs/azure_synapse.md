# Azure Synapse

Adding the spark-excel library to the Spark workspace will enable reading
and writing of Excel files to an Azure Storage Account.

At the time of writing, the following libraries have to be added to the
workspace and then configured for each Spark Pool.

Each library can be downloaded from [Maven Central](https://search.maven.org)
(thanks Sonatype!).

* spark-excel_2.12-3.1.2_0.16.5-pre2.jar
* log4j-core-2.17.2.jar
* log4j-api-2.17.2.jar
* xmlbeans-5.0.3.jar
* poi-ooxml-lite-5.2.2.jar
* commons-collections4-4.4.jar

Once those have been applied, the Excel files can be read into a dataframe like so:

```
excel_path = "abfss://<container>@<storage account>.dfs.core.windows.net/<path to excel>" 
df = (spark.read
    .format("excel")
    .load(excel_path)
)
display(df)
```
