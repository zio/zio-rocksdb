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

object Deserializer extends CollectionDeserializers {
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

  val chars: Deserializer[Any, Chunk[Char]] =
    bytes.mapMResult(fromByteBuffer(char))

  val doubles: Deserializer[Any, Chunk[Double]] =
    bytes.mapMResult(fromByteBuffer(double))

  def floats: Deserializer[Any, Chunk[Float]] =
    bytes.mapMResult(fromByteBuffer(float))

  def ints: Deserializer[Any, Chunk[Int]] =
    bytes.mapMResult(fromByteBuffer(int))

  val longs: Deserializer[Any, Chunk[Long]] =
    bytes.mapMResult(fromByteBuffer(long))

  val shorts: Deserializer[Any, Chunk[Short]] =
    bytes.mapMResult(fromByteBuffer(short))
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
