package com.crealytics.spark.excel
import scala.util.{Success, Try}

object Utils {
  implicit class RichTry[T](t: Try[T]) {
    def toEither: Either[Throwable, T] = t.transform(s => Success(Right(s)), f => Success(Left(f))).get
  }
}
