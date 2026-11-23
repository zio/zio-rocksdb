package zio.rocksdb

/**
 * The reason a `Deserializer` failed. Each case has a `message`.
 */
sealed abstract class DeserializeError(val message: String)

/**
 * The cases of `DeserializeError`.
 */
object DeserializeError {

  /**
   * A `Throwable` raised while decoding. The `message` is the message of the `Throwable`. The library's own
   * decoders never produce this case. It is for custom decoders that call code which can throw.
   */
  final case class ChunkError(underlying: Throwable) extends DeserializeError(underlying.getMessage)

  /**
   * The input has fewer bytes than the decoder needs. `got` is the length of the whole input, and `expected`
   * is the number of bytes the decoder needs.
   */
  final case class TooShort(got: Int, expected: Int)
      extends DeserializeError(s"chunk too short, was $got, expected: $expected")

  /**
   * A byte is not one of the accepted values. `got` is the byte that was read, and `expected` lists the
   * accepted values. `Deserializer.boolean` produces this case for any byte other than 0 and 1.
   */
  final case class UnexpectedByte(got: Byte, expected: List[Byte])
      extends DeserializeError(s"""Got $got, expected one of: ${expected.mkString(",")}""")
}
