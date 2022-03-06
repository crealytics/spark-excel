/** Copyright 2016 - 2021 Martin Mauch (@nightscape)
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
  * the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package com.crealytics.spark.v2.excel

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql._
import org.scalatest.wordspec.AnyWordSpec

class DataFrameWriterApiComplianceSuite extends AnyWordSpec with DataFrameSuiteBase with LocalFileTestingUtilities {

  private def readSimpleCsv = {
    spark.read
      .format("csv")
      .option("delimiter", ";")
      .option("header", "true")
      .option("path", "src/test/resources/v2readwritetest/simple_csv/simple.csv")
      .load()
  }

  "excel v2" can {

    val writeModes = Seq(SaveMode.Overwrite, SaveMode.Append)

    for (writeMode <- writeModes) {
      s"write a dataframe to xlsx with ${writeMode.toString}" in withExistingCleanTempDir("v2") { targetDir =>
        // create a df from csv then write as xlsx
        val dfCsv = readSimpleCsv

        dfCsv.write
          .format("excel")
          .option("path", targetDir)
          .option("header", value = true)
          .mode(writeMode)
          .save()

        val listOfFiles = getFilteredFileList(targetDir, "xlsx")
        // scalastyle:off println
        println(listOfFiles)
        // scalastyle:on println
        assert(listOfFiles.length == 1)
      }
      s"write a dataframe to xlsx with ${writeMode.toString} (partitioned)" in withExistingCleanTempDir("v2") {
        targetDir =>
          assume(spark.sparkContext.version >= "3.0.1")
          // create a df from csv then write as xlsx
          val dfCsv = readSimpleCsv

          dfCsv.write
            .partitionBy("col1")
            .format("excel")
            .option("path", targetDir)
            .option("header", value = true)
            .mode(writeMode)
            .save()

          val listOfFolders = getListOfFolders(targetDir)
          // scalastyle:off println

          println(listOfFolders)
          assert(listOfFolders.length == 2)
          for (folder <- listOfFolders) {
            val listOfFiles = getFilteredFileList(folder.getAbsolutePath, "xlsx")
            println(listOfFiles)
            assert(listOfFiles.nonEmpty)
          }

      }
    }

  }

}
