package zio.rocksdb

import zio.Chunk
import zio.test.Assertion._
import zio.test.{ assertM, check, Gen }
import zio.test.{ Gen, ZIOSpecDefault }

object SerdeSpec extends ZIOSpecDefault {
  override def spec = suite("SerdeSpec")(
    suite("primitive types")(
      roundtrip("boolean", Gen.boolean, Serializer.boolean, Deserializer.boolean),
      roundtrip("byte", Gen.byte, Serializer.byte, Deserializer.byte),
      roundtrip("char", Gen.char, Serializer.char, Deserializer.char),
      roundtrip("double", Gen.double(Double.MinValue, Double.MaxValue), Serializer.double, Deserializer.double),
      roundtrip("float", Gen.float, Serializer.float, Deserializer.float),
      roundtrip("int", Gen.int, Serializer.int, Deserializer.int),
      roundtrip("long", Gen.long, Serializer.long, Deserializer.long),
      roundtrip("short", Gen.short, Serializer.short, Deserializer.short)
    ),
    suite("tuples")(
      roundtrip(
        "tuple2",
        Gen.int zip Gen.int,
        Serializer.tuple2(Serializer.int, Serializer.int),
        Deserializer.tuple2(Deserializer.int, Deserializer.int)
      ),
      roundtrip(
        "tuple3",
        Gen.int zip Gen.int zip Gen.int,
        Serializer.tuple3(Serializer.int, Serializer.int, Serializer.int),
        Deserializer.tuple3(Deserializer.int, Deserializer.int, Deserializer.int)
      ),
      roundtrip(
        "tuple4",
        Gen.int zip Gen.int zip Gen.int zip Gen.int,
        Serializer.tuple4(Serializer.int, Serializer.int, Serializer.int, Serializer.int),
        Deserializer.tuple4(Deserializer.int, Deserializer.int, Deserializer.int, Deserializer.int)
      ),
      roundtrip(
        "tuple5",
        Gen.int zip Gen.int zip Gen.int zip Gen.int zip Gen.int,
        Serializer.tuple5(Serializer.int, Serializer.int, Serializer.int, Serializer.int, Serializer.int),
        Deserializer
          .tuple5(Deserializer.int, Deserializer.int, Deserializer.int, Deserializer.int, Deserializer.int)
      )
    ),
    suite("collections")(
      roundtrip(
        "lists",
        Gen.listOf(Gen.int),
        Serializer.ints[List],
        Deserializer.list(Deserializer.chunk(Deserializer.int))
      ),
      roundtrip(
        "maps",
        Gen.listOf(Gen.int zip Gen.short).map(_.toMap),
        Serializer.map(Serializer.int, Serializer.short),
        Deserializer.map(Deserializer.chunk(Deserializer.int zip Deserializer.short))
      )
    )
  )

  private def roundtrip[R, A](label: String, gen: Gen[R, A], ser: Serializer[R, A], deser: Deserializer[R, A]) =
    test(label)(check(gen) { x =>
      assertM(ser(x).flatMap(deser.decode))(equalTo(Result(x, Chunk.empty)))
    })
}
