package com.crealytics.spark.v2.excel

import java.io.File
import java.util.Random

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
  def deleteDirectory(folderToDelete: File): Unit = {
    val allContents = folderToDelete.listFiles
    if (allContents != null) for (file <- allContents) {
      deleteDirectory(file)
    }
    folderToDelete.delete
  }

  /** fixture that creates a temporary folder and deletes it after test completion */
  def withExistingCleanTempDir(name: String): (String => Unit) => Unit = {

    def fixture(testCode: String => Unit): Unit = {

      val directory: File = createTemporaryDirectory(name)

      try testCode(directory.getPath)
      finally deleteDirectory(directory)
    }

    fixture
  }

  def createTemporaryDirectory(name: String): File = {
    val rnd = new util.Random
    val randomValue = rnd.nextInt(10000)
    val directory = new File(s"tmp/$randomValue/$name")
    if (directory.exists) {
      deleteDirectory(directory)
    }
    if (!directory.exists) {
      directory.mkdirs()
    }
    directory
  }
}
