package com.crealytics.spark.excel

import org.apache.spark.sql.types._

sealed trait SheetXHeader {
  def namesAndTypes(excerpt: Seq[SheetRow], inferSchema: Boolean = false): Seq[StructField] = {
    val dataTypes = if (inferSchema) {
      val cellTypes: Seq[Seq[DataType]] = dataRows(excerpt).map(_.map(_.sparkDataType))
      InferSchema(cellTypes)
    } else {
      // By default fields are assumed to be StringType
      excerpt.map(_.size).reduceOption(math.max) match {
        case None => Array()
        case Some(maxCellsPerRow) => {
          (0 until maxCellsPerRow).map(_ => StringType: DataType).toArray
        }
      }
    }
    val columnNames = colNames(excerpt, dataTypes)
    columnNames.zipAll(dataTypes, null, StringType).filter(_._1 != null).map {
      case (colName, dataType) =>
        StructField(name = colName, dataType = dataType, nullable = true)
    }
  }
  def dataRows(excerpt: Seq[SheetRow]): Seq[SheetRow]
  def colNames(excerpt: Seq[SheetRow], dataTypes: Seq[DataType]): Seq[String]
}
class SheetWithHeader() extends SheetXHeader {
  def dataRows(excerpt: Seq[SheetRow]): Seq[SheetRow] = excerpt.tail
  def colNames(excerpt: Seq[SheetRow], dataTypes: Seq[DataType]): Seq[String] = {
    require(excerpt.nonEmpty, "If headers=true, every file is required to have at least a header row")
    val headerCells = excerpt.head

    val headerNames = headerCells.map(_.colName)
    val duplicates = {
      val nonNullHeaderNames = headerNames.filter(_ != null)
      nonNullHeaderNames.groupBy(identity).filter(_._2.size > 1).keySet
    }
    headerCells.zipWithIndex.map {
      case (cell, index) =>
        val value = cell.colName
        if (value == null || value.isEmpty) {
          // When there are empty strings or nulls, put the index as the suffix.
          s"_c$index"
        } else if (duplicates.contains(value)) {
          // When there are duplicates, put the index as the suffix.
          s"$value$index"
        } else {
          value
        }
    }
  }
}
class SheetNoHeader() extends SheetXHeader {

  def dataRows(excerpt: Seq[SheetRow]): Seq[SheetRow] = excerpt
  def colNames(excerpt: Seq[SheetRow], dataTypes: Seq[DataType]): Seq[String] = {
    dataTypes.indices.map { index =>
      // Uses default column names, "_c#" where # is its position of fields
      // when header option is disabled.
      s"_c$index"
    }
  }
}
