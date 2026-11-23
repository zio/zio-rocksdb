package zio.rocksdb

import java.{ lang => jlang }
import java.nio.ByteBuffer
import zio.{ Chunk, Ref, Schedule, UIO, ZIO }

/**
 * Decodes a value of type `A` from the front of a `Chunk[Byte]`. `decode` returns the value together with the
 * bytes that were not read, as a `Result`, or fails with a `DeserializeError`. The combinators `zip` and
 * `flatMap` read the bytes in sequence.
 */
trait Deserializer[-R, +A] { self =>

  /**
   * Decodes a value from `bytes` and returns it with the bytes that were not read.
   */
  def decode(bytes: Bytes): ZIO[R, DeserializeError, Result[A]]

  /**
   * Transforms the decoded value with `f`. The remainder is unchanged.
   */
  final def map[B](f: A => B): Deserializer[R, B] =
    mapResult(_.map(f))

  /**
   * Transforms the whole `Result` with `f`, both the value and the remainder.
   */
  final def mapResult[B](f: Result[A] => Result[B]): Deserializer[R, B] =
    new Deserializer[R, B] {
      def decode(bytes: Bytes): ZIO[R, DeserializeError, Result[B]] = self.decode(bytes).map(f)
    }

  /**
   * Transforms the decoded value with the effect `f`. A failure of `f` is returned as the decode error.
   */
  final def mapM[R1 <: R, B](f: A => ZIO[R1, DeserializeError, B]): Deserializer[R1, B] =
    mapMResult(_.mapM(f))

  /**
   * Transforms the whole `Result` with the effect `f`, both the value and the remainder.
   */
  final def mapMResult[R1 <: R, B](f: Result[A] => ZIO[R1, DeserializeError, Result[B]]): Deserializer[R1, B] =
    new Deserializer[R1, B] {
      def decode(bytes: Bytes): ZIO[R1, DeserializeError, Result[B]] = self.decode(bytes).flatMap(f)
    }

  /**
   * Decodes a value, then builds the next decoder from it with `f` and runs that decoder on the bytes that
   * were not read.
   */
  final def flatMap[R1 <: R, B](f: A => Deserializer[R1, B]): Deserializer[R1, B] =
    new Deserializer[R1, B] {
      def decode(bytes: Bytes): ZIO[R1, DeserializeError, Result[B]] =
        self.decode(bytes).flatMap {
          case Result(a, bytes) =>
            f(a).decode(bytes)
        }
    }

  /**
   * Decodes this value, then decodes `that` from the bytes that were not read, and returns both values as a
   * pair.
   */
  final def zip[R1 <: R, B](that: Deserializer[R1, B]): Deserializer[R1, (A, B)] = zipWith(that)(_ -> _)

  /**
   * Decodes this value, then decodes `that` from the bytes that were not read, and combines the two values
   * with `f`.
   */
  final def zipWith[R1 <: R, B, C](that: Deserializer[R1, B])(f: (A, B) => C): Deserializer[R1, C] =
    new Deserializer[R1, C] {
      def decode(bytes: Bytes): ZIO[R1, DeserializeError, Result[C]] =
        self.decode(bytes).flatMap { case Result(a, bytes) => that.decode(bytes).map(_.map(f(a, _))) }
    }

}

/**
 * Constructors and combinators for `Deserializer`.
 */
object Deserializer extends CollectionDeserializers with TupleDeserializers {

  /**
   * Creates a `Deserializer` from a function that decodes a `Chunk[Byte]`.
   */
  def apply[R, A](f: Bytes => ZIO[R, DeserializeError, Result[A]]): Deserializer[R, A] =
    new Deserializer[R, A] {
      def decode(bytes: Bytes): ZIO[R, DeserializeError, Result[A]] = f(bytes)
    }

}

private[rocksdb] trait CollectionDeserializers extends PrimitiveDeserializers {

  /**
   * Returns all the remaining bytes, and leaves an empty remainder.
   */
  val bytes: Deserializer[Any, Bytes] =
    Deserializer[Any, Bytes](bs => ZIO.succeed(Result(bs, Chunk.empty)))

  /**
   * Returns all the remaining bytes as an `Array[Byte]`, and leaves an empty remainder.
   */
  val byteArray: Deserializer[Any, Array[Byte]] =
    bytes.map(_.toArray)

  /**
   * Decodes elements one after another until the next element fails with `DeserializeError.TooShort`, and
   * returns them as a `Chunk`. Any other error fails the whole decode. The bytes after the last element are
   * the remainder.
   */
  def chunk[R, A](a: Deserializer[R, A]): Deserializer[R, Chunk[A]] =
    bytes.mapMResult(fromByteBuffer(a))

  /**
   * Decodes a `Chunk` of elements and returns them as a `List`, in the same order.
   */
  def list[R, A](as: Deserializer[R, Chunk[A]]): Deserializer[R, List[A]] =
    as.map(_.toList)

  /**
   * Decodes a `Chunk` of key-value pairs and returns a `Map`. When a key appears more than once, the last pair
   * wins.
   */
  def map[R, K, A](as: Deserializer[R, Chunk[(K, A)]]): Deserializer[R, Map[K, A]] =
    as.map(as => Map(as.toList: _*))

  /**
   * Decodes a `Chunk` of elements and returns a `Set`, which drops duplicate elements.
   */
  def set[R, A](as: Deserializer[R, Chunk[A]]): Deserializer[R, Set[A]] =
    as.map(as => Set(as.toList: _*))

  /**
   * Decodes a `Chunk` of elements and returns a `Vector`, in the same order.
   */
  def vector[R, A](as: Deserializer[R, Chunk[A]]): Deserializer[R, Vector[A]] =
    as.map(as => Vector(as.toList: _*))
}

private[rocksdb] trait TupleDeserializers {

  /**
   * Decodes a tuple of 2 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple2[R, A, B](implicit sera: Deserializer[R, A], serb: Deserializer[R, B]): Deserializer[R, (A, B)] =
    sera zip serb

  /**
   * Decodes a tuple of 3 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple3[RR, A, B, C](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C]
  ): Deserializer[RR, (A, B, C)] =
    (sera zip serb zip serc).map { case ((a, b), c) => (a, b, c) }

  /**
   * Decodes a tuple of 4 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple4[RR, A, B, C, D](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D]
  ): Deserializer[RR, (A, B, C, D)] =
    (sera zip serb zip serc zip serd).map { case (((a, b), c), d) => (a, b, c, d) }

  /**
   * Decodes a tuple of 5 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple5[RR, A, B, C, D, E](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E]
  ): Deserializer[RR, (A, B, C, D, E)] =
    (sera zip serb zip serc zip serd zip sere).map { case ((((a, b), c), d), e) => (a, b, c, d, e) }

  /**
   * Decodes a tuple of 6 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple6[RR, A, B, C, D, E, F](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F]
  ): Deserializer[RR, (A, B, C, D, E, F)] =
    (sera zip serb zip serc zip serd zip sere zip serf).map {
      case (((((a, b), c), d), e), f) => (a, b, c, d, e, f)
    }

  /**
   * Decodes a tuple of 7 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple7[RR, A, B, C, D, E, F, G](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G]
  ): Deserializer[RR, (A, B, C, D, E, F, G)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg).map {
      case ((((((a, b), c), d), e), f), g) => (a, b, c, d, e, f, g)
    }

  /**
   * Decodes a tuple of 8 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple8[RR, A, B, C, D, E, F, G, H](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh).map {
      case (((((((a, b), c), d), e), f), g), h) => (a, b, c, d, e, f, g, h)
    }

  /**
   * Decodes a tuple of 9 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple9[RR, A, B, C, D, E, F, G, H, I](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H],
    seri: Deserializer[RR, I]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H, I)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh zip seri).map {
      case ((((((((a, b), c), d), e), f), g), h), i) => (a, b, c, d, e, f, g, h, i)
    }

  /**
   * Decodes a tuple of 10 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple10[RR, A, B, C, D, E, F, G, H, I, J](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H],
    seri: Deserializer[RR, I],
    serj: Deserializer[RR, J]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H, I, J)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh zip seri zip serj).map {
      case (((((((((a, b), c), d), e), f), g), h), i), j) => (a, b, c, d, e, f, g, h, i, j)
    }

  /**
   * Decodes a tuple of 11 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple11[RR, A, B, C, D, E, F, G, H, I, J, K](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H],
    seri: Deserializer[RR, I],
    serj: Deserializer[RR, J],
    serk: Deserializer[RR, K]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H, I, J, K)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh zip seri zip serj zip serk).map {
      case ((((((((((a, b), c), d), e), f), g), h), i), j), k) => (a, b, c, d, e, f, g, h, i, j, k)
    }

  /**
   * Decodes a tuple of 12 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple12[RR, A, B, C, D, E, F, G, H, I, J, K, L](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H],
    seri: Deserializer[RR, I],
    serj: Deserializer[RR, J],
    serk: Deserializer[RR, K],
    serl: Deserializer[RR, L]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh zip seri zip serj zip serk zip serl).map {
      case (((((((((((a, b), c), d), e), f), g), h), i), j), k), l) => (a, b, c, d, e, f, g, h, i, j, k, l)
    }

  /**
   * Decodes a tuple of 13 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple13[RR, A, B, C, D, E, F, G, H, I, J, K, L, M](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H],
    seri: Deserializer[RR, I],
    serj: Deserializer[RR, J],
    serk: Deserializer[RR, K],
    serl: Deserializer[RR, L],
    serm: Deserializer[RR, M]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh zip seri zip serj zip serk zip serl zip serm).map {
      case ((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m) => (a, b, c, d, e, f, g, h, i, j, k, l, m)
    }

  /**
   * Decodes a tuple of 14 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple14[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H],
    seri: Deserializer[RR, I],
    serj: Deserializer[RR, J],
    serk: Deserializer[RR, K],
    serl: Deserializer[RR, L],
    serm: Deserializer[RR, M],
    sern: Deserializer[RR, N]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh zip seri zip serj zip serk zip serl zip serm zip sern).map {
      case (((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n) =>
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n)
    }

  /**
   * Decodes a tuple of 15 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple15[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H],
    seri: Deserializer[RR, I],
    serj: Deserializer[RR, J],
    serk: Deserializer[RR, K],
    serl: Deserializer[RR, L],
    serm: Deserializer[RR, M],
    sern: Deserializer[RR, N],
    sero: Deserializer[RR, O]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh zip seri zip serj zip serk zip serl zip serm zip sern zip sero).map {
      case ((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o) =>
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
    }

  /**
   * Decodes a tuple of 16 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple16[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H],
    seri: Deserializer[RR, I],
    serj: Deserializer[RR, J],
    serk: Deserializer[RR, K],
    serl: Deserializer[RR, L],
    serm: Deserializer[RR, M],
    sern: Deserializer[RR, N],
    sero: Deserializer[RR, O],
    serp: Deserializer[RR, P]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh zip seri zip serj zip serk zip serl zip serm zip sern zip sero zip serp).map {
      case (((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o), p) =>
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
    }

  /**
   * Decodes a tuple of 17 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple17[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H],
    seri: Deserializer[RR, I],
    serj: Deserializer[RR, J],
    serk: Deserializer[RR, K],
    serl: Deserializer[RR, L],
    serm: Deserializer[RR, M],
    sern: Deserializer[RR, N],
    sero: Deserializer[RR, O],
    serp: Deserializer[RR, P],
    serq: Deserializer[RR, Q]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh zip seri zip serj zip serk zip serl zip serm zip sern zip sero zip serp zip serq).map {
      case ((((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o), p), q) =>
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
    }

  /**
   * Decodes a tuple of 18 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple18[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H],
    seri: Deserializer[RR, I],
    serj: Deserializer[RR, J],
    serk: Deserializer[RR, K],
    serl: Deserializer[RR, L],
    serm: Deserializer[RR, M],
    sern: Deserializer[RR, N],
    sero: Deserializer[RR, O],
    serp: Deserializer[RR, P],
    serq: Deserializer[RR, Q],
    serr: Deserializer[RR, R]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh zip seri zip serj zip serk zip serl zip serm zip sern zip sero zip serp zip serq zip serr).map {
      case (((((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o), p), q), r) =>
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
    }

  /**
   * Decodes a tuple of 19 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple19[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H],
    seri: Deserializer[RR, I],
    serj: Deserializer[RR, J],
    serk: Deserializer[RR, K],
    serl: Deserializer[RR, L],
    serm: Deserializer[RR, M],
    sern: Deserializer[RR, N],
    sero: Deserializer[RR, O],
    serp: Deserializer[RR, P],
    serq: Deserializer[RR, Q],
    serr: Deserializer[RR, R],
    sers: Deserializer[RR, S]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh zip seri zip serj zip serk zip serl zip serm zip sern zip sero zip serp zip serq zip serr zip sers).map {
      case ((((((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o), p), q), r), s) =>
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
    }

  /**
   * Decodes a tuple of 20 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple20[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H],
    seri: Deserializer[RR, I],
    serj: Deserializer[RR, J],
    serk: Deserializer[RR, K],
    serl: Deserializer[RR, L],
    serm: Deserializer[RR, M],
    sern: Deserializer[RR, N],
    sero: Deserializer[RR, O],
    serp: Deserializer[RR, P],
    serq: Deserializer[RR, Q],
    serr: Deserializer[RR, R],
    sers: Deserializer[RR, S],
    sert: Deserializer[RR, T]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh zip seri zip serj zip serk zip serl zip serm zip sern zip sero zip serp zip serq zip serr zip sers zip sert).map {
      case (((((((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o), p), q), r), s), t) =>
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
    }

  /**
   * Decodes a tuple of 21 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple21[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H],
    seri: Deserializer[RR, I],
    serj: Deserializer[RR, J],
    serk: Deserializer[RR, K],
    serl: Deserializer[RR, L],
    serm: Deserializer[RR, M],
    sern: Deserializer[RR, N],
    sero: Deserializer[RR, O],
    serp: Deserializer[RR, P],
    serq: Deserializer[RR, Q],
    serr: Deserializer[RR, R],
    sers: Deserializer[RR, S],
    sert: Deserializer[RR, T],
    seru: Deserializer[RR, U]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh zip seri zip serj zip serk zip serl zip serm zip sern zip sero zip serp zip serq zip serr zip sers zip sert zip seru).map {
      case ((((((((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o), p), q), r), s), t), u) =>
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
    }

  /**
   * Decodes a tuple of 22 elements. Each element is decoded in order from the bytes that the previous element did not read.
   */
  def tuple22[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E],
    serf: Deserializer[RR, F],
    serg: Deserializer[RR, G],
    serh: Deserializer[RR, H],
    seri: Deserializer[RR, I],
    serj: Deserializer[RR, J],
    serk: Deserializer[RR, K],
    serl: Deserializer[RR, L],
    serm: Deserializer[RR, M],
    sern: Deserializer[RR, N],
    sero: Deserializer[RR, O],
    serp: Deserializer[RR, P],
    serq: Deserializer[RR, Q],
    serr: Deserializer[RR, R],
    sers: Deserializer[RR, S],
    sert: Deserializer[RR, T],
    seru: Deserializer[RR, U],
    serv: Deserializer[RR, V]
  ): Deserializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] =
    (sera zip serb zip serc zip serd zip sere zip serf zip serg zip serh zip seri zip serj zip serk zip serl zip serm zip sern zip sero zip serp zip serq zip serr zip sers zip sert zip seru zip serv).map {
      case (((((((((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o), p), q), r), s), t), u), v) =>
        (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
    }
}

private[rocksdb] trait PrimitiveDeserializers extends DeserializerUtilityFunctions {

  /**
   * Decodes one byte. The byte 0 gives `false` and the byte 1 gives `true`. Any other byte fails with
   * `DeserializeError.UnexpectedByte`.
   */
  lazy val boolean: Deserializer[Any, Boolean] =
    byte.mapM {
      case 0 => ZIO.succeed(false)
      case 1 => ZIO.succeed(true)
      case x => ZIO.fail(DeserializeError.UnexpectedByte(x, List(0, 1)))
    }

  /**
   * Decodes one byte as a `Byte`. The input must hold at least one byte, or the decode fails with
   * `DeserializeError.TooShort`.
   */
  val byte: Deserializer[Any, Byte] =
    fromByteBuffer[Byte](1, _.get(0))

  /**
   * Decodes two bytes as a big-endian `Char`.
   */
  val char: Deserializer[Any, Char] =
    fromByteBuffer[Char](java.lang.Character.BYTES, _.getChar())

  /**
   * Decodes eight bytes as a big-endian `Double`.
   */
  val double: Deserializer[Any, Double] =
    fromByteBuffer[Double](java.lang.Double.BYTES, _.getDouble())

  /**
   * Decodes four bytes as a big-endian `Float`.
   */
  val float: Deserializer[Any, Float] =
    fromByteBuffer[Float](java.lang.Float.BYTES, _.getFloat())

  /**
   * Decodes four bytes as a big-endian `Int`.
   */
  val int: Deserializer[Any, Int] =
    fromByteBuffer[Int](jlang.Integer.BYTES, _.getInt())

  /**
   * Decodes eight bytes as a big-endian `Long`.
   */
  val long: Deserializer[Any, Long] =
    fromByteBuffer[Long](jlang.Long.BYTES, _.getLong())

  /**
   * Decodes two bytes as a big-endian `Short`.
   */
  val short: Deserializer[Any, Short] =
    fromByteBuffer[Short](jlang.Short.BYTES, _.getShort())
}

private[rocksdb] trait DeserializerUtilityFunctions {

  /**
   * Decodes the first `n` bytes and passes them to `f` as a big-endian `ByteBuffer`. The input must hold at
   * least `n` bytes, or the decode fails with `DeserializeError.TooShort`.
   */
  def fromByteBuffer[A](n: Int, f: ByteBuffer => A): Deserializer[Any, A] =
    new Deserializer[Any, A] {
      def decode(bytes: Bytes): ZIO[Any, DeserializeError, Result[A]] = {
        val (head, tail) = bytes.splitAt(n)
        if (head.size < n)
          ZIO.fail(DeserializeError.TooShort(bytes.length, n))
        else
          ZIO.succeed {
            val buf = ByteBuffer.wrap(head.toArray)
            Result(f(buf), tail)
          }
      }
    }

  /**
   * Decodes elements with `deser` from `bytes` until `deser` fails with `DeserializeError.TooShort`. Returns
   * the decoded elements in a `Chunk`, with the bytes that were not read as the remainder.
   */
  def fromByteBuffer[R, A](
    deser: Deserializer[R, A]
  )(bytes: Result[Bytes]): ZIO[R, DeserializeError, Result[Chunk[A]]] =
    (for {
      bs <- Ref.make(bytes.value)
      as <- Ref.make[Chunk[A]](Chunk.empty)
      _ <- bs.get
            .flatMap(deser.decode)
            .flatMap {
              case Result(a, bytes) => bs.set(bytes) *> as.update(_ :+ a)
            }
            .repeatOrElseEither[R, Any, DeserializeError, Unit](Schedule.forever, {
              case (DeserializeError.TooShort(_, _), _) => ZIO.unit
              case (error, _)                           => ZIO.fail(error)
            })
      result <- as.get.zipWith(bs.get)(Result.apply)
    } yield result)
}
