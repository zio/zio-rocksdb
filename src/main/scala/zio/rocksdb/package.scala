package zio

package object rocksdb {

  /**
   * A sequence of bytes. Serializers produce `Bytes`, and deserializers read it.
   */
  type Bytes = Chunk[Byte]
}
