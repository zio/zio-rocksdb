package zio.rocksdb

import zio.ZIO

final case class Result[+A](value: A, remainder: Bytes) extends Product with Serializable {
  def map[B](f: A => B): Result[B] =
    Result(f(value), remainder)

  def mapM[R, E, B](f: A => ZIO[R, E, B]): ZIO[R, E, Result[B]] =
    f(value).map(Result(_, remainder))
}
