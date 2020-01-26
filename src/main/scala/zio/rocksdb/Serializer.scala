package zio.rocksdb

import zio.{ Chunk, URIO, UIO }
import java.{ lang => jlang }
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

trait Serializer[-R, -A] { self =>
  def apply(a: A): URIO[R, Bytes]

  final def contramap[B](f: B => A): Serializer[R, B] =
    new Serializer[R, B] {
      def apply(b: B): URIO[R, Bytes] = self(f(b))
    }

  final def divideWith[B, C](that: Serializer[R, C])(f: B => (A, C)): Serializer[R, B] =
    new Serializer[R, B] {
      def apply(b: B): URIO[R, Bytes] = {
        val (a, c) = f(b)
        self(a).zipWith(that(c))(_ ++ _)
      }
    }

  final def divide[B](that: Serializer[R, B]): Serializer[R, (A, B)] = divideWith(that)(identity[(A, B)])

  final def chooseWith[B, C](that: Serializer[R, C])(f: B => Either[A, C]): Serializer[R, B] =
    new Serializer[R, B] {
      def apply(b: B): URIO[R, Bytes] =
        f(b).fold(self.apply, that.apply)
    }

  final def choose[B](that: Serializer[R, B]): Serializer[R, Either[A, B]] = chooseWith(that)(identity[Either[A, B]])
}

object Serializer extends CollectionSerializers {
  def apply[A](f: A => Bytes): Serializer[Any, A] =
    new Serializer[Any, A] {
      def apply(a: A): UIO[Bytes] = URIO.succeed(f(a))
    }

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
    Serializer[Bytes](identity)

  val byteArray: Serializer[Any, Array[Byte]] =
    bytes.contramap(Chunk.fromArray)
}

private[rocksdb] trait PrimitiveSerializers extends SerializerUtilityFunctions {
  lazy val boolean: Serializer[Any, Boolean] =
    byte.contramap(if (_) 1 else 0)

  val byte: Serializer[Any, Byte] =
    Serializer[Byte](Chunk.single)

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
  private[rocksdb] def fromByteBuffer[A](n0: Int, mutate: (ByteBuffer, A) => Any): Serializer[Any, A] =
    new Serializer[Any, A] {
      var buf: ByteBuffer = null
      def apply(a: A): UIO[Bytes] = {
        val n = n0 max 1
        URIO { 
          if (buf eq null)
            buf = ByteBuffer.allocate(n)
          buf.position(0)
          mutate(buf, a)
          Chunk.fromArray(buf.array)
        }
      }
    }
}
