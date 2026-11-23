package zio.rocksdb.iterator

/**
 * The direction in which an iterator moves through the keys.
 */
sealed trait Direction

/**
 * The directions for `RocksDB.newIterator`.
 */
object Direction {

  /**
   * Moves to larger keys, in ascending key order.
   */
  case object Forward extends Direction

  /**
   * Moves to smaller keys, in descending key order.
   */
  case object Backward extends Direction
}
