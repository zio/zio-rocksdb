package zio.rocksdb

import zio.{ Chunk, UIO, URIO, ZIO }
import java.{ lang => jlang }
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * Encodes a value of type `A` into bytes. Encoding can use the environment `R`, and it cannot fail, so the
 * effect type is `URIO`.
 */
trait Serializer[-R, -A] { self =>

  /**
   * Encodes `a` into bytes.
   */
  def apply(a: A): URIO[R, Bytes]

  /**
   * Returns a serializer for `B` that converts each value with `f` and then encodes the result.
   */
  final def contramap[B](f: B => A): Serializer[R, B] =
    new Serializer[R, B] {
      def apply(b: B): URIO[R, Bytes] = self(f(b))
    }

  /**
   * Returns a serializer for `B`. Each value is split by `f` into an `A` and a `C`. The bytes of the `A` are
   * written first, followed by the bytes of the `C`.
   */
  final def divideWith[R1 <: R, B, C](that: Serializer[R1, C])(f: B => (A, C)): Serializer[R1, B] =
    new Serializer[R1, B] {
      def apply(b: B): URIO[R1, Bytes] = {
        val (a, c) = f(b)
        self(a).zipWith(that(c))(_ ++ _)
      }
    }

  /**
   * Returns a serializer for pairs. The bytes of the first element are written first, followed by the bytes
   * of the second element.
   */
  final def divide[R1 <: R, B](that: Serializer[R1, B]): Serializer[R1, (A, B)] = divideWith(that)(identity[(A, B)])

  /**
   * Returns a serializer for `B`. The function `f` picks `Left` to encode with this serializer, or `Right` to
   * encode with `that`. The output has no marker for the side that was chosen.
   */
  final def chooseWith[R1 <: R, B, C](that: Serializer[R1, C])(f: B => Either[A, C]): Serializer[R1, B] =
    new Serializer[R1, B] {
      def apply(b: B): URIO[R1, Bytes] =
        f(b).fold(self.apply, that.apply)
    }

  /**
   * Returns a serializer for `Either` values. A `Left` is encoded with this serializer and a `Right` with
   * `that`. The output has no marker for the side that was chosen.
   */
  final def choose[R1 <: R, B](that: Serializer[R1, B]): Serializer[R1, Either[A, B]] =
    chooseWith(that)(identity[Either[A, B]])
}

/**
 * Constructors and combinators for `Serializer`.
 */
object Serializer extends CollectionSerializers with TupleSerializers {

  /**
   * Creates a `Serializer` from a function that encodes a value into bytes.
   */
  def apply[R, A](f: A => URIO[R, Bytes]): Serializer[R, A] =
    new Serializer[R, A] {
      def apply(a: A): URIO[R, Bytes] = f(a)
    }

  /**
   * Returns a serializer that ignores its input and always encodes `a` with `enc`. The value `a` is evaluated
   * each time the serializer runs.
   */
  def const[R, A](a: => A)(enc: Serializer[R, A]): Serializer[R, A] =
    Serializer[R, A](_ => enc(a))

  /**
   * Encodes a `Left` with `l` and a `Right` with `r`. The output has no marker for the side that was chosen,
   * so the bytes alone cannot tell the two cases apart.
   */
  def either[R, A, B](l: Serializer[R, A], r: Serializer[R, B]): Serializer[R, Either[A, B]] =
    Serializer[R, Either[A, B]](_.fold(l.apply, r.apply))

  /**
   * Encodes any value as zero bytes.
   */
  val empty: Serializer[Any, Any] =
    Serializer[Any, Any](_ => ZIO.succeed(Chunk.empty))

  /**
   * Encodes `None` as zero bytes and `Some(a)` with `enc`. The output has no marker for the case that was
   * written, so `None` and an encoding of `Some` with no bytes cannot be told apart.
   */
  def option[R, A](enc: Serializer[R, A]): Serializer[R, Option[A]] =
    Serializer[R, Option[A]](_.fold[URIO[R, Bytes]](empty.apply(()))(enc.apply))

  /**
   * Encodes a `String` as UTF-8 bytes, with no length prefix.
   */
  def string: Serializer[Any, String] =
    bytes.contramap(s => Chunk.fromArray(s.getBytes(StandardCharsets.UTF_8)))
}

private[rocksdb] trait CollectionSerializers extends PrimitiveSerializers {

  /**
   * Writes a `Bytes` value unchanged.
   */
  val bytes: Serializer[Any, Bytes] =
    Serializer[Any, Bytes](ZIO.succeed(_))

  /**
   * Writes an `Array[Byte]` unchanged.
   */
  val byteArray: Serializer[Any, Array[Byte]] =
    bytes.contramap(Chunk.fromArray)

  /**
   * Writes each `Char` of the collection as two big-endian bytes, in the iteration order of the collection.
   */
  def chars[F[_]](implicit ev: F[Char] <:< Iterable[Char]): Serializer[Any, F[Char]] =
    fromByteBuffer[F, Char](jlang.Character.BYTES, _ putChar _)

  /**
   * Writes each `Double` of the collection as eight big-endian bytes, in the iteration order of the collection.
   */
  def doubles[F[_]](implicit ev: F[Double] <:< Iterable[Double]): Serializer[Any, F[Double]] =
    fromByteBuffer[F, Double](jlang.Double.BYTES, _ putDouble _)

  /**
   * Writes each `Float` of the collection as four big-endian bytes, in the iteration order of the collection.
   */
  def floats[F[_]](implicit ev: F[Float] <:< Iterable[Float]): Serializer[Any, F[Float]] =
    fromByteBuffer[F, Float](jlang.Float.BYTES, _ putFloat _)

  /**
   * Writes each `Int` of the collection as four big-endian bytes, in the iteration order of the collection.
   */
  def ints[F[_]](implicit ev: F[Int] <:< Iterable[Int]): Serializer[Any, F[Int]] =
    fromByteBuffer[F, Int](jlang.Integer.BYTES, _ putInt _)

  /**
   * Writes each `Long` of the collection as eight big-endian bytes, in the iteration order of the collection.
   */
  def longs[F[_]](implicit ev: F[Long] <:< Iterable[Long]): Serializer[Any, F[Long]] =
    fromByteBuffer[F, Long](jlang.Long.BYTES, _ putLong _)

  /**
   * Writes each `Short` of the collection as two big-endian bytes, in the iteration order of the collection.
   */
  def shorts[F[_]](implicit ev: F[Short] <:< Iterable[Short]): Serializer[Any, F[Short]] =
    fromByteBuffer[F, Short](jlang.Short.BYTES, _ putShort _)

  /**
   * Encodes each element with `enc` and writes the bytes one after another. The output has no count or length
   * prefix.
   */
  def chunk[R, A](enc: Serializer[R, A]): Serializer[R, Chunk[A]] =
    list(enc).contramap(_.toList)

  /**
   * Encodes each element with `enc` and writes the bytes one after another. The output has no count or length
   * prefix.
   */
  def list[R, A](enc: Serializer[R, A]): Serializer[R, List[A]] =
    Serializer[R, List[A]](ZIO.foldLeft(_)(Chunk.empty: Bytes) { case (as, a) => enc(a).map(as ++ _) })

  /**
   * Encodes each key-value pair with `encK` and `encV`, writing the bytes of each key followed by its value,
   * one pair after another. The output has no count or length prefix.
   */
  def map[R, K, V](encK: Serializer[R, K], encV: Serializer[R, V]): Serializer[R, Map[K, V]] =
    list(encK divide encV).contramap[Map[K, V]](_.toList)

  /**
   * Encodes each element with `enc` and writes the bytes one after another. The output has no count or length
   * prefix.
   */
  def vector[R, A](enc: Serializer[R, A]): Serializer[R, Vector[A]] =
    list(enc).contramap(_.toList)
}

private[rocksdb] trait TupleSerializers {

  /**
   * Encodes a tuple of 2 elements. The bytes of each element are written in order, one after another.
   */
  def tuple2[R, A, B](implicit sera: Serializer[R, A], serb: Serializer[R, B]): Serializer[R, (A, B)] =
    sera divide serb

  /**
   * Encodes a tuple of 3 elements. The bytes of each element are written in order, one after another.
   */
  def tuple3[RR, A, B, C](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C]
  ): Serializer[RR, (A, B, C)] =
    (sera divide serb divide serc).contramap { case (a, b, c) => ((a, b), c) }

  /**
   * Encodes a tuple of 4 elements. The bytes of each element are written in order, one after another.
   */
  def tuple4[RR, A, B, C, D](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D]
  ): Serializer[RR, (A, B, C, D)] =
    (sera divide serb divide serc divide serd).contramap { case (a, b, c, d) => (((a, b), c), d) }

  /**
   * Encodes a tuple of 5 elements. The bytes of each element are written in order, one after another.
   */
  def tuple5[RR, A, B, C, D, E](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E]
  ): Serializer[RR, (A, B, C, D, E)] =
    (sera divide serb divide serc divide serd divide sere).contramap { case (a, b, c, d, e) => ((((a, b), c), d), e) }

  /**
   * Encodes a tuple of 6 elements. The bytes of each element are written in order, one after another.
   */
  def tuple6[RR, A, B, C, D, E, F](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F]
  ): Serializer[RR, (A, B, C, D, E, F)] =
    (sera divide serb divide serc divide serd divide sere divide serf).contramap {
      case (a, b, c, d, e, f) => (((((a, b), c), d), e), f)
    }

  /**
   * Encodes a tuple of 7 elements. The bytes of each element are written in order, one after another.
   */
  def tuple7[RR, A, B, C, D, E, F, G](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G]
  ): Serializer[RR, (A, B, C, D, E, F, G)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg).contramap {
      case (a, b, c, d, e, f, g) => ((((((a, b), c), d), e), f), g)
    }

  /**
   * Encodes a tuple of 8 elements. The bytes of each element are written in order, one after another.
   */
  def tuple8[RR, A, B, C, D, E, F, G, H](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H]
  ): Serializer[RR, (A, B, C, D, E, F, G, H)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh).contramap {
      case (a, b, c, d, e, f, g, h) => (((((((a, b), c), d), e), f), g), h)
    }

  /**
   * Encodes a tuple of 9 elements. The bytes of each element are written in order, one after another.
   */
  def tuple9[RR, A, B, C, D, E, F, G, H, I](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H],
    seri: Serializer[RR, I]
  ): Serializer[RR, (A, B, C, D, E, F, G, H, I)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh divide seri).contramap {
      case (a, b, c, d, e, f, g, h, i) => ((((((((a, b), c), d), e), f), g), h), i)
    }

  /**
   * Encodes a tuple of 10 elements. The bytes of each element are written in order, one after another.
   */
  def tuple10[RR, A, B, C, D, E, F, G, H, I, J](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H],
    seri: Serializer[RR, I],
    serj: Serializer[RR, J]
  ): Serializer[RR, (A, B, C, D, E, F, G, H, I, J)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh divide seri divide serj).contramap {
      case (a, b, c, d, e, f, g, h, i, j) => (((((((((a, b), c), d), e), f), g), h), i), j)
    }

  /**
   * Encodes a tuple of 11 elements. The bytes of each element are written in order, one after another.
   */
  def tuple11[RR, A, B, C, D, E, F, G, H, I, J, K](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H],
    seri: Serializer[RR, I],
    serj: Serializer[RR, J],
    serk: Serializer[RR, K]
  ): Serializer[RR, (A, B, C, D, E, F, G, H, I, J, K)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh divide seri divide serj divide serk).contramap {
      case (a, b, c, d, e, f, g, h, i, j, k) => ((((((((((a, b), c), d), e), f), g), h), i), j), k)
    }

  /**
   * Encodes a tuple of 12 elements. The bytes of each element are written in order, one after another.
   */
  def tuple12[RR, A, B, C, D, E, F, G, H, I, J, K, L](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H],
    seri: Serializer[RR, I],
    serj: Serializer[RR, J],
    serk: Serializer[RR, K],
    serl: Serializer[RR, L]
  ): Serializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh divide seri divide serj divide serk divide serl).contramap {
      case (a, b, c, d, e, f, g, h, i, j, k, l) => (((((((((((a, b), c), d), e), f), g), h), i), j), k), l)
    }

  /**
   * Encodes a tuple of 13 elements. The bytes of each element are written in order, one after another.
   */
  def tuple13[RR, A, B, C, D, E, F, G, H, I, J, K, L, M](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H],
    seri: Serializer[RR, I],
    serj: Serializer[RR, J],
    serk: Serializer[RR, K],
    serl: Serializer[RR, L],
    serm: Serializer[RR, M]
  ): Serializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh divide seri divide serj divide serk divide serl divide serm).contramap {
      case (a, b, c, d, e, f, g, h, i, j, k, l, m) => ((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m)
    }

  /**
   * Encodes a tuple of 14 elements. The bytes of each element are written in order, one after another.
   */
  def tuple14[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H],
    seri: Serializer[RR, I],
    serj: Serializer[RR, J],
    serk: Serializer[RR, K],
    serl: Serializer[RR, L],
    serm: Serializer[RR, M],
    sern: Serializer[RR, N]
  ): Serializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh divide seri divide serj divide serk divide serl divide serm divide sern).contramap {
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n) =>
        (((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n)
    }

  /**
   * Encodes a tuple of 15 elements. The bytes of each element are written in order, one after another.
   */
  def tuple15[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H],
    seri: Serializer[RR, I],
    serj: Serializer[RR, J],
    serk: Serializer[RR, K],
    serl: Serializer[RR, L],
    serm: Serializer[RR, M],
    sern: Serializer[RR, N],
    sero: Serializer[RR, O]
  ): Serializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh divide seri divide serj divide serk divide serl divide serm divide sern divide sero).contramap {
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) =>
        ((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o)
    }

  /**
   * Encodes a tuple of 16 elements. The bytes of each element are written in order, one after another.
   */
  def tuple16[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H],
    seri: Serializer[RR, I],
    serj: Serializer[RR, J],
    serk: Serializer[RR, K],
    serl: Serializer[RR, L],
    serm: Serializer[RR, M],
    sern: Serializer[RR, N],
    sero: Serializer[RR, O],
    serp: Serializer[RR, P]
  ): Serializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh divide seri divide serj divide serk divide serl divide serm divide sern divide sero divide serp).contramap {
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) =>
        (((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o), p)
    }

  /**
   * Encodes a tuple of 17 elements. The bytes of each element are written in order, one after another.
   */
  def tuple17[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H],
    seri: Serializer[RR, I],
    serj: Serializer[RR, J],
    serk: Serializer[RR, K],
    serl: Serializer[RR, L],
    serm: Serializer[RR, M],
    sern: Serializer[RR, N],
    sero: Serializer[RR, O],
    serp: Serializer[RR, P],
    serq: Serializer[RR, Q]
  ): Serializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh divide seri divide serj divide serk divide serl divide serm divide sern divide sero divide serp divide serq).contramap {
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q) =>
        ((((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o), p), q)
    }

  /**
   * Encodes a tuple of 18 elements. The bytes of each element are written in order, one after another.
   */
  def tuple18[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H],
    seri: Serializer[RR, I],
    serj: Serializer[RR, J],
    serk: Serializer[RR, K],
    serl: Serializer[RR, L],
    serm: Serializer[RR, M],
    sern: Serializer[RR, N],
    sero: Serializer[RR, O],
    serp: Serializer[RR, P],
    serq: Serializer[RR, Q],
    serr: Serializer[RR, R]
  ): Serializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh divide seri divide serj divide serk divide serl divide serm divide sern divide sero divide serp divide serq divide serr).contramap {
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r) =>
        (((((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o), p), q), r)
    }

  /**
   * Encodes a tuple of 19 elements. The bytes of each element are written in order, one after another.
   */
  def tuple19[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H],
    seri: Serializer[RR, I],
    serj: Serializer[RR, J],
    serk: Serializer[RR, K],
    serl: Serializer[RR, L],
    serm: Serializer[RR, M],
    sern: Serializer[RR, N],
    sero: Serializer[RR, O],
    serp: Serializer[RR, P],
    serq: Serializer[RR, Q],
    serr: Serializer[RR, R],
    sers: Serializer[RR, S]
  ): Serializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh divide seri divide serj divide serk divide serl divide serm divide sern divide sero divide serp divide serq divide serr divide sers).contramap {
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s) =>
        ((((((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o), p), q), r), s)
    }

  /**
   * Encodes a tuple of 20 elements. The bytes of each element are written in order, one after another.
   */
  def tuple20[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H],
    seri: Serializer[RR, I],
    serj: Serializer[RR, J],
    serk: Serializer[RR, K],
    serl: Serializer[RR, L],
    serm: Serializer[RR, M],
    sern: Serializer[RR, N],
    sero: Serializer[RR, O],
    serp: Serializer[RR, P],
    serq: Serializer[RR, Q],
    serr: Serializer[RR, R],
    sers: Serializer[RR, S],
    sert: Serializer[RR, T]
  ): Serializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh divide seri divide serj divide serk divide serl divide serm divide sern divide sero divide serp divide serq divide serr divide sers divide sert).contramap {
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t) =>
        (((((((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o), p), q), r), s), t)
    }

  /**
   * Encodes a tuple of 21 elements. The bytes of each element are written in order, one after another.
   */
  def tuple21[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H],
    seri: Serializer[RR, I],
    serj: Serializer[RR, J],
    serk: Serializer[RR, K],
    serl: Serializer[RR, L],
    serm: Serializer[RR, M],
    sern: Serializer[RR, N],
    sero: Serializer[RR, O],
    serp: Serializer[RR, P],
    serq: Serializer[RR, Q],
    serr: Serializer[RR, R],
    sers: Serializer[RR, S],
    sert: Serializer[RR, T],
    seru: Serializer[RR, U]
  ): Serializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh divide seri divide serj divide serk divide serl divide serm divide sern divide sero divide serp divide serq divide serr divide sers divide sert divide seru).contramap {
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u) =>
        ((((((((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o), p), q), r), s), t), u)
    }

  /**
   * Encodes a tuple of 22 elements. The bytes of each element are written in order, one after another.
   */
  def tuple22[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E],
    serf: Serializer[RR, F],
    serg: Serializer[RR, G],
    serh: Serializer[RR, H],
    seri: Serializer[RR, I],
    serj: Serializer[RR, J],
    serk: Serializer[RR, K],
    serl: Serializer[RR, L],
    serm: Serializer[RR, M],
    sern: Serializer[RR, N],
    sero: Serializer[RR, O],
    serp: Serializer[RR, P],
    serq: Serializer[RR, Q],
    serr: Serializer[RR, R],
    sers: Serializer[RR, S],
    sert: Serializer[RR, T],
    seru: Serializer[RR, U],
    serv: Serializer[RR, V]
  ): Serializer[RR, (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] =
    (sera divide serb divide serc divide serd divide sere divide serf divide serg divide serh divide seri divide serj divide serk divide serl divide serm divide sern divide sero divide serp divide serq divide serr divide sers divide sert divide seru divide serv).contramap {
      case (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v) =>
        (((((((((((((((((((((a, b), c), d), e), f), g), h), i), j), k), l), m), n), o), p), q), r), s), t), u), v)
    }

}

private[rocksdb] trait PrimitiveSerializers extends SerializerUtilityFunctions {

  /**
   * Encodes `true` as the byte 1 and `false` as the byte 0.
   */
  lazy val boolean: Serializer[Any, Boolean] =
    byte.contramap(if (_) 1 else 0)

  /**
   * Encodes a `Byte` as one byte.
   */
  val byte: Serializer[Any, Byte] =
    Serializer[Any, Byte](b => ZIO.succeed(Chunk.single(b)))

  /**
   * Encodes a `Char` as two big-endian bytes.
   */
  val char: Serializer[Any, Char] =
    fromByteBuffer(jlang.Character.BYTES, _ putChar _)

  /**
   * Encodes a `Double` as eight big-endian bytes.
   */
  val double: Serializer[Any, Double] =
    fromByteBuffer(jlang.Double.BYTES, _ putDouble _)

  /**
   * Encodes a `Float` as four big-endian bytes.
   */
  val float: Serializer[Any, Float] =
    fromByteBuffer(jlang.Float.BYTES, _ putFloat _)

  /**
   * Encodes an `Int` as four big-endian bytes.
   */
  val int: Serializer[Any, Int] =
    fromByteBuffer(jlang.Integer.BYTES, _ putInt _)

  /**
   * Encodes a `Long` as eight big-endian bytes.
   */
  val long: Serializer[Any, Long] =
    fromByteBuffer(jlang.Long.BYTES, _ putLong _)

  /**
   * Encodes a `Short` as two big-endian bytes.
   */
  val short: Serializer[Any, Short] =
    fromByteBuffer(jlang.Short.BYTES, _ putShort _)
}

private[rocksdb] trait SerializerUtilityFunctions {
  private[rocksdb] def fromByteBuffer[A](n: Int, mutate: (ByteBuffer, A) => Any): Serializer[Any, A] =
    new Serializer[Any, A] {
      def apply(a: A): UIO[Bytes] =
        ZIO.succeed {
          val buf: ByteBuffer = ByteBuffer.allocate(n)
          mutate(buf, a)
          Chunk.fromArray(buf.array)
        }
    }

  private[rocksdb] def fromByteBuffer[F[_], A](n: Int, mutate: (ByteBuffer, A) => Any)(
    implicit ev: F[A] <:< Iterable[A]
  ): Serializer[Any, F[A]] =
    new Serializer[Any, F[A]] {
      def apply(fa: F[A]): UIO[Bytes] =
        ZIO.succeed {
          val s   = fa.size
          val l   = n * s
          val buf = ByteBuffer.allocate(l)
          var i   = 0
          fa.foreach { a =>
            buf.clear()
            buf.position(n * i)
            mutate(buf, a)
            i += 1
          }
          Chunk.fromArray(buf.array)
        }
    }
}
