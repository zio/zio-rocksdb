package zio.rocksdb

import zio.ZIO

/**
 * A decoded value and the bytes that the decoder did not read. `remainder` holds those unread bytes.
 */
final case class Result[+A](value: A, remainder: Bytes) extends Product with Serializable {

  /**
   * Applies `f` to the value and keeps the remainder.
   */
  def map[B](f: A => B): Result[B] =
    Result(f(value), remainder)

  /**
   * Applies the effect `f` to the value and keeps the remainder. The result is a new `Result` with the value
   * that `f` returns.
   */
  def mapM[R, E, B](f: A => ZIO[R, E, B]): ZIO[R, E, Result[B]] =
    f(value).map(Result(_, remainder))
}
