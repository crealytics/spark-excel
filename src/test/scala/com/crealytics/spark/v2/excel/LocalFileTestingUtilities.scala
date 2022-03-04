package com.crealytics.spark.v2.excel

import java.io.File

trait LocalFileTestingUtilities {
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getListOfFolders(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isDirectory).toList
    } else {
      List[File]()
    }
  }

  def deleteDirectory(directoryToBeDeleted: File): Boolean = {
    val allContents = directoryToBeDeleted.listFiles
    if (allContents != null) for (file <- allContents) {
      deleteDirectory(file)
    }
    directoryToBeDeleted.delete
  }

  def withExistingCleanTempDir(name: String): (String => Unit) => Unit = {

    def fixture(testCode: String => Unit) = {

      val directory = new File(s"tmp/${name}")
      if (directory.exists) {
        deleteDirectory(directory)
      }
      if (!directory.exists) {
        directory.mkdirs()
      }

      try testCode(directory.getPath)
      finally deleteDirectory(directory)
    }

    fixture
  }

  def getFilteredFileList(targetDir: String, filteredByExtension: String): Seq[String] = {
    val filesInTargetDir = getListOfFiles(targetDir)
    filesInTargetDir.filter(_.getName.endsWith(filteredByExtension)).map(_.getName)
  }

}
