/*
 * Copyright 2023 Martin Mauch (@nightscape)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.crealytics.spark.excel.v2

import java.io.File
import java.nio.file.Files

trait LocalFileTestingUtilities {

  /** Returns the list of files in given directory/folder (this is not recursive)
    */
  def getListOfFiles(folder: String): List[File] = {
    val d = new File(folder)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  /** similar to getListOfFiles but filters the files by the given file extension */
  def getListOfFilesFilteredByExtension(targetDir: String, filteredByExtension: String): Seq[String] = {
    val filesInTargetDir = getListOfFiles(targetDir)
    filesInTargetDir.filter(_.getName.endsWith(filteredByExtension)).map(_.getName)
  }

  /** Returns the list of folders in given directory/folder (this is not recursive */
  def getListOfFolders(folder: String): List[File] = {
    val d = new File(folder)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isDirectory).toList
    } else {
      List[File]()
    }
  }

  /** Deletes the (non-empty) directory (recursively)
    */
  def deleteDirectoryRecursively(folderToDelete: File): Unit = {
    val allContents = folderToDelete.listFiles
    if (allContents != null) for (file <- allContents) {
      deleteDirectoryRecursively(file)
    }
    folderToDelete.delete
    ()
  }

  /** fixture that creates a temporary folder and deletes it after test completion */
  def withExistingCleanTempDir(name: String): (String => Unit) => Unit = {

    def fixture(testCode: String => Unit): Unit = {

      val directory = Files.createTempDirectory(name)

      try testCode(directory.toString)
      finally deleteDirectoryRecursively(directory.toFile)
    }

    fixture
  }

}
