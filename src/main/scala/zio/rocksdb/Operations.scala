package zio.rocksdb

import org.rocksdb.{ ColumnFamilyDescriptor, ColumnFamilyHandle, ColumnFamilyOptions }
import org.{ rocksdb => jrocks }
import zio.rocksdb.iterator.{ Direction, Position }
import zio.stream.ZStream
import zio.{ RIO, Tag, ZIO }

/**
 * Accessors for the `RocksDB` service. Each method reads the service from the ZIO environment and calls the
 * method with the same name on it. For example, `RocksDB.get(key)` needs a `RocksDB` in the environment.
 */
abstract class Operations[R <: RocksDB](implicit tagged: Tag[R]) {
  private val db: RIO[R, R] = ZIO.service[R]

  /**
   * Deletes `key` from the default ColumnFamily.
   */
  def delete(key: Array[Byte]): RIO[R, Unit] = db.flatMap(_.delete(key))

  /**
   * Deletes `key` from the ColumnFamily `cfHandle`.
   */
  def delete(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): RIO[R, Unit] = db.flatMap(_.delete(cfHandle, key))

  /**
   * Returns the value stored under `key` in the default ColumnFamily, or `None` when the key is missing.
   */
  def get(key: Array[Byte]): RIO[R, Option[Array[Byte]]] = db.flatMap(_.get(key))

  /**
   * Returns the value stored under `key` in the ColumnFamily `cfHandle`, or `None` when the key is missing.
   */
  def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): RIO[R, Option[Array[Byte]]] =
    db.flatMap(_.get(cfHandle, key))

  /**
   * Reads each key from the default ColumnFamily. The result has one entry for each key, in the same order as
   * `keys`, and an entry is `None` when its key is missing.
   */
  def multiGetAsList(keys: List[Array[Byte]]): RIO[R, List[Option[Array[Byte]]]] = db.flatMap(_.multiGetAsList(keys))

  /**
   * Reads each key from the ColumnFamily at the same position in `handles`. The result has one entry for each
   * key, in the same order as `keys`, and an entry is `None` when its key is missing.
   */
  def multiGetAsList(
    handles: List[jrocks.ColumnFamilyHandle],
    keys: List[Array[Byte]]
  ): RIO[R, List[Option[Array[Byte]]]] =
    db.flatMap(_.multiGetAsList(handles, keys))

  /**
   * Streams the entries of the default ColumnFamily in ascending key order, starting at the first key.
   */
  def newIterator: ZStream[R, Throwable, (Array[Byte], Array[Byte])] = ZStream.unwrap(db map (_.newIterator))

  /**
   * Streams the entries of the ColumnFamily `cfHandle` in ascending key order, starting at the first key.
   */
  def newIterator(cfHandle: jrocks.ColumnFamilyHandle): ZStream[R, Throwable, (Array[Byte], Array[Byte])] =
    ZStream.unwrap(db map (_.newIterator(cfHandle)))

  /**
   * Streams the entries of the ColumnFamily `cfHandle` in `direction`, starting at `position`. See
   * `zio.rocksdb.iterator.Position` for the starting points.
   */
  def newIterator(
    cfHandle: jrocks.ColumnFamilyHandle,
    direction: Direction,
    position: Position
  ): ZStream[R, Throwable, (Array[Byte], Array[Byte])] =
    ZStream.unwrap(db map (_.newIterator(cfHandle, direction, position)))

  /**
   * Streams each ColumnFamily in `cfHandles` as a pair of its handle and a stream of its entries. The pairs
   * come in the order of `cfHandles`, and each inner stream starts at the first key in ascending order.
   * Each inner stream reads a native iterator that is closed when the outer stream ends, so run the inner
   * streams while the outer stream is running (for example inside `mapZIO` or `flatMap`), not after
   * collecting the outer stream.
   */
  def newIterators(
    cfHandles: List[jrocks.ColumnFamilyHandle]
  ): ZStream[R, Throwable, (jrocks.ColumnFamilyHandle, ZStream[R, Throwable, (Array[Byte], Array[Byte])])] =
    ZStream.unwrap(db map (_.newIterators(cfHandles)))

  /**
   * Writes `value` under `key` in the default ColumnFamily.
   */
  def put(key: Array[Byte], value: Array[Byte]): RIO[R, Unit] = db.flatMap(_.put(key, value))

  /**
   * Writes `value` under `key` in the ColumnFamily `cfHandle`.
   */
  def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): RIO[R, Unit] =
    db.flatMap(_.put(cfHandle, key, value))

  /**
   * Creates a ColumnFamily from `columnFamilyDescriptor` and returns its handle.
   */
  def createColumnFamily(columnFamilyDescriptor: ColumnFamilyDescriptor): ZIO[R, Throwable, ColumnFamilyHandle] =
    db.flatMap(_.createColumnFamily(columnFamilyDescriptor))

  /**
   * Creates a ColumnFamily for each descriptor in `columnFamilyDescriptors` and returns their handles, in the
   * same order.
   */
  def createColumnFamilies(
    columnFamilyDescriptors: List[ColumnFamilyDescriptor]
  ): ZIO[R, Throwable, List[ColumnFamilyHandle]] = db.flatMap(_.createColumnFamilies(columnFamilyDescriptors))

  /**
   * Creates a ColumnFamily for each name in `columnFamilyNames`, with `columnFamilyOptions` for all of them, and
   * returns their handles, in the same order.
   */
  def createColumnFamilies(
    columnFamilyOptions: ColumnFamilyOptions,
    columnFamilyNames: List[Array[Byte]]
  ): ZIO[R, Throwable, List[ColumnFamilyHandle]] =
    db.flatMap(_.createColumnFamilies(columnFamilyOptions, columnFamilyNames))

  /**
   * Deletes the ColumnFamily that `columnFamilyHandle` refers to.
   */
  def dropColumnFamily(columnFamilyHandle: ColumnFamilyHandle): ZIO[R, Throwable, Unit] =
    db.flatMap(_.dropColumnFamily(columnFamilyHandle))

  /**
   * Deletes each ColumnFamily that the handles in `columnFamilyHandles` refer to.
   */
  def dropColumnFamilies(columnFamilyHandles: List[ColumnFamilyHandle]): ZIO[R, Throwable, Unit] =
    db.flatMap(_.dropColumnFamilies(columnFamilyHandles))
}
