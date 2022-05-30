package zio.rocksdb.iterator

import zio.Chunk

sealed trait Position

object Position {
  case object First                   extends Position
  case object Last                    extends Position
  case class Target(key: Chunk[Byte]) extends Position
}
