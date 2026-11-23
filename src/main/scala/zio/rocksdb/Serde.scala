package zio.rocksdb

import zio.{ URIO, ZIO }

/**
 * A `Serializer` and a `Deserializer` for the same type `A`. The serializer encodes an `A` with `apply`,
 * and `decode` reads it back, returning the value and the unread bytes as a `Result`.
 */
trait Serde[-R, A] extends Serializer[R, A] with Deserializer[R, A]

/**
 * Constructors for `Serde`.
 */
object Serde {

  /**
   * Combines `ser` and `deser` into one `Serde`. Encoding uses `ser`, and decoding uses `deser`. The two are
   * not checked against each other, so the caller must make them agree.
   */
  def apply[R, A](ser: Serializer[R, A], deser: Deserializer[R, A]): Serde[R, A] =
    new Serde[R, A] {
      def apply(a: A): URIO[R, Bytes]                               = ser(a)
      def decode(bytes: Bytes): ZIO[R, DeserializeError, Result[A]] = deser.decode(bytes)
    }
}
