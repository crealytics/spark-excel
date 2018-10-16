package com.crealytics.spark.excel
import scala.util.{Success, Try}

object Utils {
  implicit class RichTry[T](t: Try[T]) {
    def toEither: Either[Throwable, T] = t.transform(s => Success(Right(s)), f => Success(Left(f))).get
  }

  case class MapIncluding[K](keys: Seq[K], optionally: Seq[K] = Seq()) {
    def unapply[V](m: Map[K, V]): Option[(Seq[V], Seq[Option[V]])] =
      if (keys.forall(m.contains)) {
        Some((keys.map(m), optionally.map(m.get)))
      } else {
        None
      }
  }
}
