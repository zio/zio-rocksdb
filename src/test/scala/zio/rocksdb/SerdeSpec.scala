package zio.rocksdb

import zio.Chunk
import zio.test.Assertion._
import zio.test.{ assertM, checkM, DefaultRunnableSpec, Gen }

object SerdeSpec extends DefaultRunnableSpec {
  override def spec = suite("SerdeSpec")(
    suite("primitive types")(
      roundtrip("boolean", Gen.boolean, Serializer.boolean, Deserializer.boolean),
      roundtrip("byte", Gen.anyByte, Serializer.byte, Deserializer.byte),
      roundtrip("char", Gen.anyChar, Serializer.char, Deserializer.char),
      roundtrip("double", Gen.double(Double.MinValue, Double.MaxValue), Serializer.double, Deserializer.double),
      roundtrip("float", Gen.anyFloat, Serializer.float, Deserializer.float),
      roundtrip("int", Gen.anyInt, Serializer.int, Deserializer.int),
      roundtrip("long", Gen.anyLong, Serializer.long, Deserializer.long),
      roundtrip("short", Gen.anyShort, Serializer.short, Deserializer.short)
    ),
    suite("tuples")(
      roundtrip(
        "tuple2",
        Gen.anyInt zip Gen.anyInt,
        Serializer.tuple2(Serializer.int, Serializer.int),
        Deserializer.tuple2(Deserializer.int, Deserializer.int)
      ),
      roundtrip(
        "tuple3",
        (Gen.anyInt zip Gen.anyInt zip Gen.anyInt).map { case ((a, b), c) => (a, b, c) },
        Serializer.tuple3(Serializer.int, Serializer.int, Serializer.int),
        Deserializer.tuple3(Deserializer.int, Deserializer.int, Deserializer.int)
      ),
      roundtrip(
        "tuple4",
        (Gen.anyInt zip Gen.anyInt zip Gen.anyInt zip Gen.anyInt).map { case (((a, b), c), d) => (a, b, c, d) },
        Serializer.tuple4(Serializer.int, Serializer.int, Serializer.int, Serializer.int),
        Deserializer.tuple4(Deserializer.int, Deserializer.int, Deserializer.int, Deserializer.int)
      ),
      roundtrip(
        "tuple5",
        (Gen.anyInt zip Gen.anyInt zip Gen.anyInt zip Gen.anyInt zip Gen.anyInt).map {
          case ((((a, b), c), d), e) => (a, b, c, d, e)
        },
        Serializer.tuple5(Serializer.int, Serializer.int, Serializer.int, Serializer.int, Serializer.int),
        Deserializer
          .tuple5(Deserializer.int, Deserializer.int, Deserializer.int, Deserializer.int, Deserializer.int)
      )
    ),
    suite("collections")(
      roundtrip(
        "lists",
        Gen.listOf(Gen.anyInt),
        Serializer.ints[List],
        Deserializer.list(Deserializer.chunk(Deserializer.int))
      ),
      roundtrip(
        "maps",
        Gen.listOf(Gen.anyInt zip Gen.anyShort).map(_.toMap),
        Serializer.map(Serializer.int, Serializer.short),
        Deserializer.map(Deserializer.chunk(Deserializer.int zip Deserializer.short))
      )
    )
  )

  private def roundtrip[R, A](label: String, gen: Gen[R, A], ser: Serializer[R, A], deser: Deserializer[R, A]) =
    testM(label)(checkM(gen) { x =>
      assertM(ser(x) >>= deser.decode)(equalTo(Result(x, Chunk.empty)))
    })
}
