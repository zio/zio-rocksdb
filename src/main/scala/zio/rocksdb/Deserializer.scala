package zio.rocksdb

import zio.ZIO

trait Deserializer[-R, +A] { self =>
  def decode(bytes: Bytes): ZIO[R, DeserializeError, Result[A]]

  final def map[B](f: A => B): Deserializer[R, B] =
    new Deserializer[R, B] {
      def decode(bytes: Bytes): ZIO[R, DeserializeError, Result[B]] = self.decode(bytes).map(_.map(f))
    }

  final def flatMap[B, A1 >: A](f: A1 => Deserializer[B]): Deserializer[R, B] =
    new Deserializer[R, B] {
      def decode(bytes: Bytes): ZIO[R, DeserializeError, Result[B]] =
        self.decode(bytes).flatMap {
          case Result(a, bytes) =>
            f(a).decode(bytes)
        }
    }

  final def zip[B](that: Deserializer[R, B]): Deserializer[R, (A, B)] = zipWith(that)(_ -> _)

  final def zipWith[B, C](that: Deserializer[R, B])(f: (A, B) => C): Deserializer[R, C] =
    new Deserializer[R, C] {
      def decode(bytes: Bytes): ZIO[R, DeserializeError, Result[C]] =
        self.decode(bytes).flatMap { case Result(a, bytes) => that.decode(bytes).map(_.map(f(a, _))) }
    }

}

object Deserializer {
  def apply[R, A](f: Bytes => ZIO[R, DeserializeError, Result[A]]): Deserializer[R, A] =
    new Deserializer[R, A] {
      def decode(bytes: Any): ZIO[R, Any, Result[A]] = f(bytes)
    }
}
