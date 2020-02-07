package zio.rocksdb

import zio.ZIO

trait Deserializer[-R, +A] { self =>
  def apply(bytes: Bytes): ZIO[R, DeserializeError, Result[A]]

  final def map[B](f: A => B): Deserializer[R, B] =
    new Deserializer[R, B] {
      def apply(bytes: Bytes): ZIO[R, DeserializeError, Result[B]] = self(bytes).map(_.map(f))
    }

  final def flatMap[B](f: A => Deserializer[R, B]): Deserializer[R, B] =
    new Deserializer[R, B] {
      def apply(bytes: Bytes): ZIO[R, DeserializeError, Result[B]] =
        self(bytes).flatMap {
          case Result(a, bytes) =>
            f(a)(bytes)
        }
    }

  final def zip[B](that: Deserializer[R, B]): Deserializer[R, (A, B)] = zipWith(that)(_ -> _)

  final def zipWith[B, C](that: Deserializer[R, B])(f: (A, B) => C): Deserializer[R, C] =
    new Deserializer[R, C] {
      def apply(bytes: Bytes): ZIO[R, DeserializeError, Result[C]] =
        self(bytes).flatMap { case Result(a, bytes) => that(bytes).map(_.map(f(a, _))) }
    }

}

object Deserializer {
  def apply[R, A](f: Bytes => ZIO[R, DeserializeError, Result[A]]): Deserializer[R, A] =
    new Deserializer[R, A] {
      def apply(bytes: Any): ZIO[R, Any, Result[A]] = f(bytes)
    }
}
