package com.crealytics.spark.excel

import org.apache.poi.ss.usermodel.{Cell, Row}
import org.scalacheck.Gen
import org.scalacheck.Prop.BooleanOperators
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks

import scala.util.Try

trait RowGenerator extends MockFactory {
  private val MAX_WIDTH = 100

  protected case class GeneratedRow(start: Int, end: Int, lastCellNum: Int, row: Row)

  protected val rowGen: Gen[GeneratedRow] = for {
    startColumn <- Gen.choose(0, MAX_WIDTH - 1)
    endColumn <- Gen.choose(0, MAX_WIDTH - 1)
    lastCellNum <- Gen.choose(0, MAX_WIDTH - 1)
    row = stub[Row]
    _ = (row.getCell(_: Int)).when(*) returns stub[Cell]
    _ = row.getLastCellNum _ when () returns lastCellNum.toShort

  } yield GeneratedRow(startColumn, endColumn, lastCellNum, row)
}

class RichRowSuite extends FunSuite with PropertyChecks with RowGenerator {

  test("Invalid cell range should throw an error") {
    forAll(rowGen) { g =>
      (g.start > g.end) ==> Try {
        g.row.eachCellIterator(g.start, g.end).next()
      }.isFailure
    }
  }

  test("Valid cell range should iterate through all non-empty cells") {
    forAll(rowGen) { g =>
      (g.start <= g.end && g.start < g.lastCellNum) ==> {
        val count = g.row.eachCellIterator(g.start, g.end).size
        count === Math.min(g.end, g.lastCellNum - 1) - g.start + 1
      }
    }
  }

  test("Valid cell range should should not iterate through non-empty cells") {
    forAll(rowGen) { g =>
      (g.start <= g.end && g.start >= g.lastCellNum) ==> {
        g.row.eachCellIterator(g.start, g.end).size === 0
      }
    }
  }
}
