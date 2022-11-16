/*
 * Copyright 2022 Martin Mauch (@nightscape)
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

package com.crealytics.spark

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Timestamp

trait DataFrameSuiteBase extends DataFrameComparer {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("spark-excel session")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  def assertDataFrameEquals(df1: DataFrame, df2: DataFrame): Unit =
    assertSmallDataFrameEquality(df1, df2)

  def assertDataFrameApproximateEquals(expectedDF: DataFrame, actualDF: DataFrame, relTol: Double): Unit = {
    val e = (r1: Row, r2: Row) => {
      r1.equals(r2) || RelTolComparer.areRowsEqual(r1, r2, relTol)
    }
    assertLargeDatasetEquality[Row](
      actualDF,
      expectedDF,
      equals = e,
      ignoreNullable = false,
      ignoreColumnNames = false,
      orderedComparison = false
    )
  }

  def assertDataFrameNoOrderEquals(df1: DataFrame, df2: DataFrame): Unit =
    assertSmallDataFrameEquality(df1, df2, orderedComparison = false)
}

object RelTolComparer {

  trait ToNumeric[T] {
    def toNumeric(x: Double): T
  }
  object ToNumeric {
    implicit val doubleToDouble: ToNumeric[Double] = new ToNumeric[Double] {
      def toNumeric(x: Double): Double = x
    }
    implicit val doubleToFloat: ToNumeric[Float] = new ToNumeric[Float] {
      def toNumeric(x: Double): Float = x.toFloat
    }
    implicit val doubleToLong: ToNumeric[Long] = new ToNumeric[Long] {
      def toNumeric(x: Double): Long = x.toLong
    }
    implicit val doubleToBigDecimal: ToNumeric[BigDecimal] = new ToNumeric[BigDecimal] {
      def toNumeric(x: Double): BigDecimal = BigDecimal(x)
    }
  }

  /** Approximate equality, based on equals from [[Row]] */
  def areRowsEqual(r1: Row, r2: Row, relTol: Double): Boolean = {
    def withinRelTol[T : Numeric : ToNumeric](a: T, b: T): Boolean = {
      val num = implicitly[Numeric[T]]
      val toNum = implicitly[ToNumeric[T]]
      val absTol = num.times(toNum.toNumeric(relTol), num.max(num.abs(a), num.abs(b)))
      val diff = num.abs(num.minus(a, b))
      num.lteq(diff, absTol)
    }

    if (r1.length != r2.length) {
      return false
    } else {
      (0 until r1.length).foreach(idx => {
        if (r1.isNullAt(idx) != r2.isNullAt(idx)) {
          return false
        }

        if (!r1.isNullAt(idx)) {
          val o1 = r1.get(idx)
          val o2 = r2.get(idx)
          o1 match {
            case b1: Array[Byte] =>
              if (!java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])) {
                return false
              }

            case f1: Float =>
              if (
                java.lang.Float.isNaN(f1) !=
                  java.lang.Float.isNaN(o2.asInstanceOf[Float])
              ) {
                return false
              }
              if (!withinRelTol[Float](f1, o2.asInstanceOf[Float])) {
                return false
              }

            case d1: Double =>
              if (
                java.lang.Double.isNaN(d1) !=
                  java.lang.Double.isNaN(o2.asInstanceOf[Double])
              ) {
                return false
              }
              if (!withinRelTol[Double](d1, o2.asInstanceOf[Double])) {
                return false
              }

            case d1: java.math.BigDecimal =>
              if (!withinRelTol(BigDecimal(d1), BigDecimal(o2.asInstanceOf[java.math.BigDecimal]))) {
                return false
              }

            case t1: Timestamp =>
              if (!withinRelTol(t1.getTime, o2.asInstanceOf[Timestamp].getTime)) {
                return false
              }

            case _ =>
              if (o1 != o2) return false
          }
        }
      })
    }
    true
  }

}
