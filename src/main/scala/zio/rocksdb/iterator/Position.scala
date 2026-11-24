package zio.rocksdb.iterator

import zio.Chunk

/**
 * The starting point of an iterator. The iterator starts at the position and then moves in the chosen
 * `Direction`. Starting at `First` and moving `Forward` visits every key, and so does starting at `Last` and
 * moving `Backward`. Starting at `Last` and moving `Forward` visits only the last key, and starting at
 * `First` and moving `Backward` visits only the first key.
 */
sealed trait Position

/**
 * The starting points for `RocksDB.newIterator`.
 */
object Position {

  /**
   * The first key in ascending key order.
   */
  case object First extends Position

  /**
   * The last key in ascending key order.
   */
  case object Last extends Position

  /**
   * The first key that is greater than or equal to `key`. The iterator starts at that key, and then moves in
   * the chosen `Direction`. When no key is greater than or equal to `key`, the iterator visits no keys in
   * either direction.
   */
  final case class Target(key: Chunk[Byte]) extends Position
}
