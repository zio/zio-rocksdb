package zio.rocksdb.iterator

sealed trait Direction

object Direction {
  case object Forward  extends Direction
  case object Backward extends Direction
}
