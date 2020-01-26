package zio.rocksdb

final case class Result[+A](value: A, remainder: Bytes) extends Product with Serializable {
  def map[A1 >: A, B](f: A1 => B): Result[B] =
    Result(f(value), remainder)
}
