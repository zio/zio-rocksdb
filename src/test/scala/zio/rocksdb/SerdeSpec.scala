package zio.rocksdb

import zio.Chunk
import zio.test.Assertion._
import zio.test.{ assertZIO, check, Gen }
import zio.test.{ Gen, ZIOSpecDefault }

object SerdeSpec extends ZIOSpecDefault {
  override def spec = suite("SerdeSpec")(
    suite("primitive types")(
      roundtrip[Any, Boolean]("boolean", Gen.boolean, Serializer.boolean, Deserializer.boolean),
      roundtrip[Any, Byte]("byte", Gen.byte, Serializer.byte, Deserializer.byte),
      roundtrip[Any, Char]("char", Gen.char, Serializer.char, Deserializer.char),
      roundtrip[Any, Double](
        "double",
        Gen.double(Double.MinValue, Double.MaxValue),
        Serializer.double,
        Deserializer.double
      ),
      roundtrip[Any, Float]("float", Gen.float, Serializer.float, Deserializer.float),
      roundtrip[Any, Int]("int", Gen.int, Serializer.int, Deserializer.int),
      roundtrip[Any, Long]("long", Gen.long, Serializer.long, Deserializer.long),
      roundtrip[Any, Short]("short", Gen.short, Serializer.short, Deserializer.short)
    ),
    suite("tuples")(
      roundtrip[Any, (Int, Int)](
        "tuple2",
        Gen.int zip Gen.int,
        Serializer.tuple2(Serializer.int, Serializer.int),
        Deserializer.tuple2(Deserializer.int, Deserializer.int)
      ),
      roundtrip[Any, (Int, Int, Int)](
        "tuple3",
        Gen.int zip Gen.int zip Gen.int,
        Serializer.tuple3(Serializer.int, Serializer.int, Serializer.int),
        Deserializer.tuple3(Deserializer.int, Deserializer.int, Deserializer.int)
      ),
      roundtrip[Any, (Int, Int, Int, Int)](
        "tuple4",
        Gen.int zip Gen.int zip Gen.int zip Gen.int,
        Serializer.tuple4(Serializer.int, Serializer.int, Serializer.int, Serializer.int),
        Deserializer.tuple4(Deserializer.int, Deserializer.int, Deserializer.int, Deserializer.int)
      ),
      roundtrip[Any, (Int, Int, Int, Int, Int)](
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
      assertZIO(ser(x).flatMap(deser.decode))(equalTo(Result(x, Chunk.empty)))
    })
}
