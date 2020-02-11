package zio.rocksdb

sealed abstract class DeserializeError(val message: String)

object DeserializeError {
  final case class ChunkError(underlying: Throwable) extends DeserializeError(underlying.getMessage)
  final case class TooShort(got: Int, expected: Int)
      extends DeserializeError(s"chunk too short, was $got, expected: $expected")
  final case class UnexpectedByte(got: Byte, expected: List[Byte])
      extends DeserializeError(s"""Got $got, expected one of: ${expected.mkString(",")}""")
}
