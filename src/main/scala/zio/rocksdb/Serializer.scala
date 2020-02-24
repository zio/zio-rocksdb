package zio.rocksdb

import zio.{ Chunk, UIO, URIO }
import java.{ lang => jlang }
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

trait Serializer[-R, -A] { self =>
  def apply(a: A): URIO[R, Bytes]

  final def contramap[B](f: B => A): Serializer[R, B] =
    new Serializer[R, B] {
      def apply(b: B): URIO[R, Bytes] = self(f(b))
    }

  final def divideWith[R1 <: R, B, C](that: Serializer[R1, C])(f: B => (A, C)): Serializer[R1, B] =
    new Serializer[R1, B] {
      def apply(b: B): URIO[R1, Bytes] = {
        val (a, c) = f(b)
        self(a).zipWith(that(c))(_ ++ _)
      }
    }

  final def divide[R1 <: R, B](that: Serializer[R1, B]): Serializer[R1, (A, B)] = divideWith(that)(identity[(A, B)])

  final def chooseWith[R1 <: R, B, C](that: Serializer[R1, C])(f: B => Either[A, C]): Serializer[R1, B] =
    new Serializer[R1, B] {
      def apply(b: B): URIO[R1, Bytes] =
        f(b).fold(self.apply, that.apply)
    }

  final def choose[R1 <: R, B](that: Serializer[R1, B]): Serializer[R1, Either[A, B]] =
    chooseWith(that)(identity[Either[A, B]])
}

object Serializer extends CollectionSerializers with TupleSerializers {
  def apply[R, A](f: A => URIO[R, Bytes]): Serializer[R, A] =
    new Serializer[R, A] {
      def apply(a: A): URIO[R, Bytes] = f(a)
    }

  def const[R, A](a: => A)(enc: Serializer[R, A]): Serializer[R, A] =
    Serializer[R, A](_ => enc(a))

  def either[R, A, B](l: Serializer[R, A], r: Serializer[R, B]): Serializer[R, Either[A, B]] =
    Serializer[R, Either[A, B]](_.fold(l.apply, r.apply))

  val empty: Serializer[Any, Any] =
    Serializer[Any, Any](_ => URIO.succeed(Chunk.empty))

  def option[R, A](enc: Serializer[R, A]): Serializer[R, Option[A]] =
    Serializer[R, Option[A]](_.fold[URIO[R, Bytes]](empty.apply(()))(enc.apply))

  def string: Serializer[Any, String] =
    bytes.contramap(s => Chunk.fromArray(s.getBytes(StandardCharsets.UTF_8)))
}

private[rocksdb] trait CollectionSerializers extends PrimitiveSerializers {
  val bytes: Serializer[Any, Bytes] =
    Serializer[Any, Bytes](UIO.succeed(_))

  val byteArray: Serializer[Any, Array[Byte]] =
    bytes.contramap(Chunk.fromArray)

  def chars[F[_]](implicit ev: F[Char] <:< Iterable[Char]): Serializer[Any, F[Char]] =
    fromByteBuffer[F, Char](jlang.Character.BYTES, _ putChar _)

  def doubles[F[_]](implicit ev: F[Double] <:< Iterable[Double]): Serializer[Any, F[Double]] =
    fromByteBuffer[F, Double](jlang.Double.BYTES, _ putDouble _)

  def floats[F[_]](implicit ev: F[Float] <:< Iterable[Float]): Serializer[Any, F[Float]] =
    fromByteBuffer[F, Float](jlang.Float.BYTES, _ putFloat _)

  def ints[F[_]](implicit ev: F[Int] <:< Iterable[Int]): Serializer[Any, F[Int]] =
    fromByteBuffer[F, Int](jlang.Integer.BYTES, _ putInt _)

  def longs[F[_]](implicit ev: F[Long] <:< Iterable[Long]): Serializer[Any, F[Long]] =
    fromByteBuffer[F, Long](jlang.Long.BYTES, _ putLong _)

  def shorts[F[_]](implicit ev: F[Short] <:< Iterable[Short]): Serializer[Any, F[Short]] =
    fromByteBuffer[F, Short](jlang.Short.BYTES, _ putShort _)
}

private[rocksdb] trait TupleSerializers {
  def tuple2[R, A, B](implicit sera: Serializer[R, A], serb: Serializer[R, B]): Serializer[R, (A, B)] =
    sera divide serb

  def tuple3[R, A, B](implicit sera: Serializer[R, A], serb: Serializer[R, B]): Serializer[R, (A, B)] =
    sera divide serb

  def tuple4[RR, A, B, C](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C]
  ): Serializer[RR, (A, B, C)] =
    (sera divide serb divide serc).contramap { case (a, b, c) => ((a, b), c) }

  def tuple5[RR, A, B, C, D](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D]
  ): Serializer[RR, (A, B, C, D)] =
    (sera divide serb divide serc divide serd).contramap { case (a, b, c, d) => (((a, b), c), d) }

  def tuple6[RR, A, B, C, D, E](
    implicit sera: Serializer[RR, A],
    serb: Serializer[RR, B],
    serc: Serializer[RR, C],
    serd: Serializer[RR, D],
    sere: Serializer[RR, E]
  ): Serializer[RR, (A, B, C, D, E)] =
    (sera divide serb divide serc divide serd divide sere).contramap { case (a, b, c, d, e) => ((((a, b), c), d), e) }

  def tuple7[RR, A, B, C, D, E, F](
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

  def tuple8[RR, A, B, C, D, E, F, G](
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

  def tuple9[RR, A, B, C, D, E, F, G, H](
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

  def tuple10[RR, A, B, C, D, E, F, G, H, I](
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

  def tuple11[RR, A, B, C, D, E, F, G, H, I, J](
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

  def tuple12[RR, A, B, C, D, E, F, G, H, I, J, K](
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

  def tuple13[RR, A, B, C, D, E, F, G, H, I, J, K, L](
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

  def tuple14[RR, A, B, C, D, E, F, G, H, I, J, K, L, M](
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

  def tuple15[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N](
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

  def tuple16[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](
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

  def tuple17[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](
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

  def tuple18[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](
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

  def tuple19[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](
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

  def tuple20[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](
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

  def tuple21[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](
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

  def tuple22[RR, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](
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

}

private[rocksdb] trait PrimitiveSerializers extends SerializerUtilityFunctions {
  lazy val boolean: Serializer[Any, Boolean] =
    byte.contramap(if (_) 1 else 0)

  val byte: Serializer[Any, Byte] =
    Serializer[Any, Byte](b => UIO(Chunk.single(b)))

  val char: Serializer[Any, Char] =
    fromByteBuffer(jlang.Character.BYTES, _ putChar _)

  val double: Serializer[Any, Double] =
    fromByteBuffer(jlang.Double.BYTES, _ putDouble _)

  val float: Serializer[Any, Float] =
    fromByteBuffer(jlang.Float.BYTES, _ putFloat _)

  val int: Serializer[Any, Int] =
    fromByteBuffer(jlang.Integer.BYTES, _ putInt _)

  val long: Serializer[Any, Long] =
    fromByteBuffer(jlang.Long.BYTES, _ putLong _)

  val short: Serializer[Any, Short] =
    fromByteBuffer(jlang.Short.BYTES, _ putShort _)
}

private[rocksdb] trait SerializerUtilityFunctions {
  private[rocksdb] def fromByteBuffer[A](n: Int, mutate: (ByteBuffer, A) => Any): Serializer[Any, A] =
    new Serializer[Any, A] {
      var buf: ByteBuffer = null
      def apply(a: A): UIO[Bytes] =
        URIO {
          if (buf eq null)
            buf = ByteBuffer.allocate(n)
          buf.position(0)
          mutate(buf, a)
          Chunk.fromArray(buf.array)
        }
    }

  private[rocksdb] def fromByteBuffer[F[_], A](n: Int, mutate: (ByteBuffer, A) => Any)(
    implicit ev: F[A] <:< Iterable[A]
  ): Serializer[Any, F[A]] =
    new Serializer[Any, F[A]] {
      def apply(fa: F[A]): UIO[Bytes] =
        URIO {
          val s   = fa.size
          val l   = n * s
          val buf = ByteBuffer.allocate(l)
          var i   = 0
          fa.foreach { a =>
            buf.position(n * i)
            mutate(buf, a)
            i += 1
          }
          Chunk.fromArray(buf.array)
        }
    }
}
