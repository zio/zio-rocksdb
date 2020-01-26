package zio.rocksdb

import zio.ZIO

trait Serde[-R, A] extends Serializer[R, A] with Deserializer[R, A]

object Serde {
  def apply[R, A](ser: Serializer[R, A], deser: Deserializer[R, A]): Serde[R, A] =
    new Serde[R, A] {
      def apply(a: A): URIO[R, Bytes]                               = ser(a)
      def decode(bytes: Bytes): ZIO[R, DeserializeError, Result[A]] = deser.decode(bytes)
    }
}
