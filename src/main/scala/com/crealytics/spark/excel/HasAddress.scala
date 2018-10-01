package com.crealytics.spark.excel

import com.crealytics.spark.excel.Utils._
import org.apache.poi.ss.util.{CellRangeAddress, CellReference}

import scala.util.Try

trait HasAddress {
  val dataAddress: String
  val dataCellRef: Either[CellRangeAddress, CellReference] =
    Try(new CellReference(dataAddress)).toEither.left.map(_ => CellRangeAddress.valueOf(dataAddress))
  val startRow = dataCellRef.fold(_.getFirstRow, _.getRow)
  val startColumn = dataCellRef.fold(_.getFirstColumn, _.getCol.toInt)
  val endRow = dataCellRef.fold(_.getLastRow, _ => Int.MaxValue)
  val endColumn = dataCellRef.fold(_.getLastColumn, _ => Int.MaxValue)
}

object HasAddress {
  def parseAddress(address: String): Either[CellRangeAddress, CellReference] =
    Try(new CellReference(address)).toEither.left.map(_ => CellRangeAddress.valueOf(address))
}

case class AddressContainer(dataAddress: String) extends HasAddress
