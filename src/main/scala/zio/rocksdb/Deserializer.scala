package zio.rocksdb

import java.{ lang => jlang }
import java.nio.ByteBuffer
import zio.{ Chunk, Ref, Schedule, UIO, ZIO }

trait Deserializer[-R, +A] { self =>
  def decode(bytes: Bytes): ZIO[R, DeserializeError, Result[A]]

  final def map[B](f: A => B): Deserializer[R, B] =
    mapResult(_.map(f))

  final def mapResult[B](f: Result[A] => Result[B]): Deserializer[R, B] =
    new Deserializer[R, B] {
      def decode(bytes: Bytes): ZIO[R, DeserializeError, Result[B]] = self.decode(bytes).map(f)
    }

  final def mapM[R1 <: R, B](f: A => ZIO[R1, DeserializeError, B]): Deserializer[R1, B] =
    mapMResult(_.mapM(f))

  final def mapMResult[R1 <: R, B](f: Result[A] => ZIO[R1, DeserializeError, Result[B]]): Deserializer[R1, B] =
    new Deserializer[R1, B] {
      def decode(bytes: Bytes): ZIO[R1, DeserializeError, Result[B]] = self.decode(bytes).flatMap(f)
    }

  final def flatMap[R1 <: R, B](f: A => Deserializer[R1, B]): Deserializer[R1, B] =
    new Deserializer[R1, B] {
      def decode(bytes: Bytes): ZIO[R1, DeserializeError, Result[B]] =
        self.decode(bytes).flatMap {
          case Result(a, bytes) =>
            f(a).decode(bytes)
        }
    }

  final def zip[R1 <: R, B](that: Deserializer[R1, B]): Deserializer[R1, (A, B)] = zipWith(that)(_ -> _)

  final def zipWith[R1 <: R, B, C](that: Deserializer[R1, B])(f: (A, B) => C): Deserializer[R1, C] =
    new Deserializer[R1, C] {
      def decode(bytes: Bytes): ZIO[R1, DeserializeError, Result[C]] =
        self.decode(bytes).flatMap { case Result(a, bytes) => that.decode(bytes).map(_.map(f(a, _))) }
    }

}

object Deserializer extends CollectionDeserializers with TupleDeserializers {
  def apply[R, A](f: Bytes => ZIO[R, DeserializeError, Result[A]]): Deserializer[R, A] =
    new Deserializer[R, A] {
      def decode(bytes: Bytes): ZIO[R, DeserializeError, Result[A]] = f(bytes)
    }

}

private[rocksdb] trait CollectionDeserializers extends PrimitiveDeserializers {
  val bytes: Deserializer[Any, Bytes] =
    Deserializer[Any, Bytes](bs => UIO.succeed(Result(bs, Chunk.empty)))

  val byteArray: Deserializer[Any, Array[Byte]] =
    bytes.map(_.toArray)

  def chunk[R, A](a: Deserializer[R, A]): Deserializer[R, Chunk[A]] =
    bytes.mapMResult(fromByteBuffer(a))

  def list[R, A](as: Deserializer[R, Chunk[A]]): Deserializer[R, List[A]] =
    as.map(_.toList)

  def map[R, K, A](as: Deserializer[R, Chunk[(K, A)]]): Deserializer[R, Map[K, A]] =
    as.map(as => Map(as.toList: _*))

  def set[R, A](as: Deserializer[R, Chunk[A]]): Deserializer[R, Set[A]] =
    as.map(as => Set(as.toList: _*))

  def vector[R, A](as: Deserializer[R, Chunk[A]]): Deserializer[R, Vector[A]] =
    as.map(as => Vector(as.toList: _*))
}

private[rocksdb] trait TupleDeserializers {
  def tuple2[R, A, B](implicit sera: Deserializer[R, A], serb: Deserializer[R, B]): Deserializer[R, (A, B)] =
    sera zip serb

  def tuple3[RR, A, B, C](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C]
  ): Deserializer[RR, (A, B, C)] =
    (sera zip serb zip serc).map { case ((a, b), c) => (a, b, c) }

  def tuple4[RR, A, B, C, D](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D]
  ): Deserializer[RR, (A, B, C, D)] =
    (sera zip serb zip serc zip serd).map { case (((a, b), c), d) => (a, b, c, d) }

  def tuple5[RR, A, B, C, D, E](
    implicit sera: Deserializer[RR, A],
    serb: Deserializer[RR, B],
    serc: Deserializer[RR, C],
    serd: Deserializer[RR, D],
    sere: Deserializer[RR, E]
  ): Deserializer[RR, (A, B, C, D, E)] =
    (sera zip serb zip serc zip serd zip sere).map { case ((((a, b), c), d), e) => (a, b, c, d, e) }

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
  lazy val boolean: Deserializer[Any, Boolean] =
    byte.mapM {
      case 0 => UIO(false)
      case 1 => UIO(true)
      case x => ZIO.fail(DeserializeError.UnexpectedByte(x, List(0, 1)))
    }

  val byte: Deserializer[Any, Byte] =
    fromByteBuffer[Byte](1, _.get(0))

  val char: Deserializer[Any, Char] =
    fromByteBuffer[Char](java.lang.Character.BYTES, _.getChar())

  val double: Deserializer[Any, Double] =
    fromByteBuffer[Double](java.lang.Double.BYTES, _.getDouble())

  val float: Deserializer[Any, Float] =
    fromByteBuffer[Float](java.lang.Float.BYTES, _.getFloat())

  val int: Deserializer[Any, Int] =
    fromByteBuffer[Int](jlang.Integer.BYTES, _.getInt())

  val long: Deserializer[Any, Long] =
    fromByteBuffer[Long](jlang.Long.BYTES, _.getLong())

  val short: Deserializer[Any, Short] =
    fromByteBuffer[Short](jlang.Short.BYTES, _.getShort())
}

private[rocksdb] trait DeserializerUtilityFunctions {
  def fromByteBuffer[A](n: Int, f: ByteBuffer => A): Deserializer[Any, A] =
    new Deserializer[Any, A] {
      def decode(bytes: Bytes): ZIO[Any, DeserializeError, Result[A]] = {
        val (head, tail) = bytes.splitAt(n)
        if (head.size < n)
          ZIO.fail(DeserializeError.TooShort(bytes.length, n))
        else
          ZIO.effectTotal {
            val buf = ByteBuffer.wrap(head.toArray)
            Result(f(buf), tail)
          }
      }
    }

  def fromByteBuffer[R, A](
    deser: Deserializer[R, A]
  )(bytes: Result[Bytes]): ZIO[R, DeserializeError, Result[Chunk[A]]] =
    for {
      bs <- Ref.make(bytes.value)
      as <- Ref.make[Chunk[A]](Chunk.empty)
      _ <- bs.get
            .flatMap(deser.decode)
            .flatMap {
              case Result(a, bytes) => bs.set(bytes) *> as.update(_ + a)
            }
            .repeatOrElseEither[R, Any, DeserializeError, Unit](Schedule.forever, {
              case (DeserializeError.TooShort(_, _), _) => ZIO.unit
              case (error, _)                           => ZIO.fail(error)
            })
      result <- as.get.zipWith(bs.get)(Result.apply)
    } yield result

}
