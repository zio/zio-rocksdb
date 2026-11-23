package zio.rocksdb

import org.rocksdb.{ ColumnFamilyDescriptor, ColumnFamilyHandle, ColumnFamilyOptions, RocksIterator }
import org.{ rocksdb => jrocks }
import zio._
import zio.rocksdb.iterator.{ Direction, Position }
import zio.stream.{ Stream, ZStream }

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

/**
 * The RocksDB service: a key-value store that can hold several ColumnFamilies. Build a layer with
 * `RocksDB.live`, which opens the database and closes it when the layer's scope closes. Methods that read or
 * write return a `Task`, and methods that iterate return a `ZStream`. Every failure is a `Throwable`.
 *
 * {{{
 * import zio._
 * import zio.rocksdb._
 * import java.nio.charset.StandardCharsets.UTF_8
 *
 * // Writes one key and reads it back from the RocksDB service in the environment.
 * val program: RIO[RocksDB, Option[String]] =
 *   for {
 *     _     <- RocksDB.put("name".getBytes(UTF_8), "ZIO".getBytes(UTF_8))
 *     value <- RocksDB.get("name".getBytes(UTF_8))
 *   } yield value.map(new String(_, UTF_8))
 * }}}
 *
 * @see [[https://zio.dev/zio-rocksdb/ ZIO RocksDB]]
 */
trait RocksDB {

  /**
   * Delete a key from the default ColumnFamily in the database.
   */
  def delete(key: Array[Byte]): Task[Unit]

  /**
   * Delete a key from a specific ColumnFamily in the database.
   */
  def delete(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Unit]

  /**
   * Flushes the memory table of the default ColumnFamily to files, as configured by `flushOptions`. To flush
   * other ColumnFamilies, pass their handles to the `flush` overloads that take a handle.
   */
  def flush(flushOptions: jrocks.FlushOptions): Task[Unit]

  /**
   * Flush all memory table data.
   */
  def flush(flushOptions: jrocks.FlushOptions, columnFamilyHandle: jrocks.ColumnFamilyHandle): Task[Unit]

  /**
   * Flush multiple column families.
   */
  def flush(flushOptions: jrocks.FlushOptions, columnFamilyHandles: List[ColumnFamilyHandle]): Task[Unit]

  /**
   * Flush the WAL memory buffer to the file
   */
  def flushWal(sync: Boolean): Task[Unit]

  /**
   * Retrieve a key from the default ColumnFamily in the database.
   */
  def get(key: Array[Byte]): Task[Option[Array[Byte]]]

  /**
   * Retrieve a key from a specific ColumnFamily in the database.
   */
  def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Option[Array[Byte]]]

  /**
   * Returns the ColumnFamily handles held by this instance. Every `open` method of `RocksDB.Live` stores an empty
   * list, including the methods that take ColumnFamily descriptors, so the result is always empty for an instance
   * built by those methods.
   */
  def initialHandles: Task[List[jrocks.ColumnFamilyHandle]]

  /**
   * Retrieve multiple keys from the default ColumnFamily in the database. The resulting list
   * corresponds (positionally) to the list of keys passed to the function.
   */
  def multiGetAsList(keys: List[Array[Byte]]): Task[List[Option[Array[Byte]]]]

  /**
   * Retrieve multiple keys from specific ColumnFamilies in the database. The resulting list
   * corresponds (positionally) to the list of keys passed to the function.
   */
  def multiGetAsList(
    handles: List[jrocks.ColumnFamilyHandle],
    keys: List[Array[Byte]]
  ): Task[List[Option[Array[Byte]]]]

  /**
   * Scans the default ColumnFamily in the database and emits the results as a `ZStream`.
   */
  def newIterator: Stream[Throwable, (Array[Byte], Array[Byte])]

  /**
   * Scans the default ColumnFamily in the database and emits the results as a `ZStream`.
   *
   * {{{
   * import zio._
   * import zio.rocksdb._
   * import zio.rocksdb.iterator._
   * import zio.stream.ZStream
   * import java.nio.charset.StandardCharsets.UTF_8
   *
   * // Streams the keys from "b" upward, in ascending order.
   * val keysFromB: ZStream[RocksDB, Throwable, String] =
   *   ZStream.serviceWithStream[RocksDB](
   *     _.newIterator(Direction.Forward, Position.Target(Chunk.fromArray("b".getBytes(UTF_8))))
   *       .map { case (key, _) => new String(key, UTF_8) }
   *   )
   * }}}
   */
  def newIterator(
    direction: Direction,
    position: Position
  ): Stream[Throwable, (Array[Byte], Array[Byte])]

  /**
   * Scans a specific ColumnFamily in the database and emits the results as a `ZStream`.
   */
  def newIterator(cfHandle: jrocks.ColumnFamilyHandle): Stream[Throwable, (Array[Byte], Array[Byte])]

  /**
   * Scans a specific ColumnFamily in the database by specifying direction and position and
   * emits the results as a `ZStream`.
   */
  def newIterator(
    cfHandle: jrocks.ColumnFamilyHandle,
    direction: Direction,
    position: Position
  ): Stream[Throwable, (Array[Byte], Array[Byte])]

  /**
   * Scans multiple ColumnFamilies in the database and emits the results in multiple streams,
   * whereas the streams themselves are also emitted in a `ZStream`.
   */
  def newIterators(
    cfHandles: List[jrocks.ColumnFamilyHandle]
  ): Stream[Throwable, (jrocks.ColumnFamilyHandle, Stream[Throwable, (Array[Byte], Array[Byte])])]

  /**
   * Writes a key to the default ColumnFamily in the database.
   */
  def put(key: Array[Byte], value: Array[Byte]): Task[Unit]

  /**
   * Writes `value` under `key` in the ColumnFamily `cfHandle`.
   *
   * {{{
   * import zio._
   * import zio.rocksdb._
   * import org.rocksdb.ColumnFamilyDescriptor
   * import java.nio.charset.StandardCharsets.UTF_8
   *
   * // Creates the "users" ColumnFamily, then writes one entry to it.
   * val addUser: RIO[RocksDB, Unit] =
   *   RocksDB.createColumnFamily(new ColumnFamilyDescriptor("users".getBytes(UTF_8))).flatMap { users =>
   *     RocksDB.put(users, "id".getBytes(UTF_8), "ZIO".getBytes(UTF_8))
   *   }
   * }}}
   */
  def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Task[Unit]

  /**
   * Creates a new ColumnFamily from ColumnFamilyDescriptor
   */
  def createColumnFamily(columnFamilyDescriptor: ColumnFamilyDescriptor): Task[ColumnFamilyHandle]

  /**
   * Creates ColumnFamilies from a list of ColumnFamilyDescriptors
   */
  def createColumnFamilies(
    columnFamilyDescriptors: List[ColumnFamilyDescriptor]
  ): Task[List[ColumnFamilyHandle]]

  /**
   * Creates ColumnFamilies from a list of ColumnFamilyNames and ColumnFamilyOptions
   */
  def createColumnFamilies(
    columnFamilyOptions: ColumnFamilyOptions,
    columnFamilyNames: List[Array[Byte]]
  ): Task[List[ColumnFamilyHandle]]

  /**
   * Deletes a ColumnFamily
   */
  def dropColumnFamily(columnFamilyHandle: ColumnFamilyHandle): Task[Unit]

  /**
   * Deletes ColumnFamilies given a list of ColumnFamilyHandles
   */
  def dropColumnFamilies(columnFamilyHandles: List[ColumnFamilyHandle]): Task[Unit]

  /**
   * Applies the writes in `writeBatch` to the database as one batch, with `writeOptions`.
   */
  def write(writeOptions: jrocks.WriteOptions, writeBatch: WriteBatch): Task[Unit]
}

/**
 * Constructors for the `RocksDB` service, and the operations that read the service from the ZIO environment.
 */
object RocksDB extends Operations[RocksDB] {

  /**
   * The `RocksDB` implementation over a `jrocks.RocksDB`. Each read and write call to the native library runs
   * inside `ZIO.attempt`, so an exception that the native library throws becomes a failure of the effect.
   */
  class Live protected (db: jrocks.RocksDB, cfHandles: List[jrocks.ColumnFamilyHandle]) extends RocksDB {

    /**
     * Creates a ColumnFamily from `columnFamilyDescriptor` and returns its handle.
     */
    def createColumnFamily(columnFamilyDescriptor: ColumnFamilyDescriptor): Task[ColumnFamilyHandle] =
      ZIO.attempt(db.createColumnFamily(columnFamilyDescriptor))

    /**
     * Creates a ColumnFamily for each descriptor and returns their handles, in the same order.
     */
    def createColumnFamilies(
      columnFamilyDescriptors: List[ColumnFamilyDescriptor]
    ): Task[List[ColumnFamilyHandle]] =
      ZIO.attempt(db.createColumnFamilies(columnFamilyDescriptors.asJava).asScala.toList)

    /**
     * Creates a ColumnFamily for each name, with `columnFamilyOptions`, and returns their handles, in the same
     * order.
     */
    def createColumnFamilies(
      columnFamilyOptions: ColumnFamilyOptions,
      columnFamilyNames: List[Array[Byte]]
    ): Task[List[ColumnFamilyHandle]] =
      ZIO.attempt(db.createColumnFamilies(columnFamilyOptions, columnFamilyNames.asJava).asScala.toList)

    /**
     * Deletes `key` from the default ColumnFamily.
     */
    def delete(key: Array[Byte]): Task[Unit] =
      ZIO.attempt(db.delete(key))

    /**
     * Deletes `key` from the ColumnFamily `cfHandle`.
     */
    def delete(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Unit] =
      ZIO.attempt(db.delete(cfHandle, key))

    /**
     * Deletes the ColumnFamily that `columnFamilyHandle` refers to.
     */
    def dropColumnFamily(columnFamilyHandle: ColumnFamilyHandle): Task[Unit] =
      ZIO.attempt(db.dropColumnFamily(columnFamilyHandle))

    /**
     * Deletes each ColumnFamily that the handles in `columnFamilyHandles` refer to.
     */
    def dropColumnFamilies(columnFamilyHandles: List[ColumnFamilyHandle]): Task[Unit] =
      ZIO.attempt(db.dropColumnFamilies(columnFamilyHandles.asJava))

    /**
     * Returns the value stored under `key` in the default ColumnFamily, or `None` when the key is missing.
     */
    def get(key: Array[Byte]): Task[Option[Array[Byte]]] =
      ZIO.attempt(Option(db.get(key)))

    /**
     * Returns the value stored under `key` in the ColumnFamily `cfHandle`, or `None` when the key is missing.
     */
    def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Option[Array[Byte]]] =
      ZIO.attempt(Option(db.get(cfHandle, key)))

    /**
     * Flushes the memory table of the default ColumnFamily to files, as configured by `flushOptions`.
     */
    def flush(flushOptions: jrocks.FlushOptions): Task[Unit] =
      ZIO.attempt(db.flush(flushOptions))

    /**
     * Flushes the memory table of the ColumnFamily `columnFamilyHandle` to files, as configured by
     * `flushOptions`.
     */
    def flush(flushOptions: jrocks.FlushOptions, columnFamilyHandle: jrocks.ColumnFamilyHandle): Task[Unit] =
      ZIO.attempt(db.flush(flushOptions, columnFamilyHandle))

    /**
     * Flushes the memory tables of the ColumnFamilies in `columnFamilyHandles` to files, as configured by
     * `flushOptions`.
     */
    def flush(flushOptions: jrocks.FlushOptions, columnFamilyHandles: List[ColumnFamilyHandle]): Task[Unit] =
      ZIO.attempt(db.flush(flushOptions, columnFamilyHandles.asJava))

    /**
     * Writes the buffered write-ahead log to the file system. When `sync` is `true`, the file is also synced to
     * storage.
     */
    def flushWal(sync: Boolean): Task[Unit] =
      ZIO.attempt(db.flushWal(sync))

    /**
     * Returns the ColumnFamily handles held by this instance. Every `open` method stores an empty list, including
     * the methods that take ColumnFamily descriptors, so the result is always empty for an instance built by them.
     */
    def initialHandles: Task[List[jrocks.ColumnFamilyHandle]] =
      ZIO.succeed(cfHandles)

    /**
     * Reads each key from the default ColumnFamily. The result has one entry for each key, in the same order as
     * `keys`, and an entry is `None` when its key is missing.
     */
    def multiGetAsList(keys: List[Array[Byte]]): Task[List[Option[Array[Byte]]]] =
      ZIO.attempt(db.multiGetAsList(keys.asJava).asScala.toList.map(Option(_)))

    /**
     * Reads each key from the ColumnFamily at the same position in `handles`. The result has one entry for each
     * key, in the same order as `keys`, and an entry is `None` when its key is missing.
     */
    def multiGetAsList(
      handles: List[jrocks.ColumnFamilyHandle],
      keys: List[Array[Byte]]
    ): Task[List[Option[Array[Byte]]]] =
      ZIO.attempt {
        db.multiGetAsList(handles.asJava, keys.asJava).asScala.toList.map(Option(_))
      }

    private def drainIterator(direction: Direction, position: Position)(
      it: jrocks.RocksIterator
    ): Stream[Throwable, (Array[Byte], Array[Byte])] =
      ZStream.fromZIO(ZIO.attempt(set(it, position))).drain ++
        ZStream.fromZIO(ZIO.attempt(it.isValid)).flatMap { valid =>
          if (!valid) ZStream.empty
          else
            ZStream.fromZIO(ZIO.attempt(it.key() -> it.value())) ++ ZStream.repeatZIOOption {
              ZIO.attempt {
                step(it, direction)

                if (!it.isValid) ZIO.fail(None)
                else ZIO.succeed(it.key() -> it.value())
              }.mapError(Some(_)).flatten
            }
        }

    private def set(it: jrocks.RocksIterator, position: Position): Unit =
      position match {
        case Position.Last        => it.seekToLast()
        case Position.First       => it.seekToFirst()
        case Position.Target(key) => it.seek(key.toArray)
      }

    private def step(it: jrocks.RocksIterator, direction: Direction): Unit =
      direction match {
        case Direction.Forward  => it.next()
        case Direction.Backward => it.prev()
      }

    /**
     * Streams the entries of the default ColumnFamily in `direction`, starting at `position`. See
     * `zio.rocksdb.iterator.Position` for the starting points.
     */
    def newIterator(
      direction: Direction,
      position: Position
    ): Stream[Throwable, (Array[Byte], Array[Byte])] =
      ZStream
        .acquireReleaseWith(ZIO.attempt(db.newIterator()))(it => ZIO.succeed(it.close()))
        .flatMap(drainIterator(direction, position))

    /**
     * Streams the entries of the default ColumnFamily in ascending key order, starting at the first key.
     */
    def newIterator: Stream[Throwable, (Array[Byte], Array[Byte])] =
      newIterator(Direction.Forward, Position.First)

    /**
     * Streams the entries of the ColumnFamily `cfHandle` in ascending key order, starting at the first key.
     */
    def newIterator(cfHandle: jrocks.ColumnFamilyHandle): Stream[Throwable, (Array[Byte], Array[Byte])] =
      ZStream
        .acquireReleaseWith(ZIO.attempt(db.newIterator(cfHandle)))(it => ZIO.succeed(it.close()))
        .flatMap(drainIterator(Direction.Forward, Position.First))

    /**
     * Streams the entries of the ColumnFamily `cfHandle` in `direction`, starting at `position`. See
     * `zio.rocksdb.iterator.Position` for the starting points.
     */
    def newIterator(
      cfHandle: ColumnFamilyHandle,
      direction: Direction,
      position: Position
    ): Stream[Throwable, (Array[Byte], Array[Byte])] =
      ZStream
        .acquireReleaseWith(ZIO.attempt(db.newIterator(cfHandle)))(it => ZIO.succeed(it.close()))
        .flatMap(drainIterator(direction, position))

    /**
     * Streams each ColumnFamily in `cfHandles` as a pair of its handle and a stream of its entries. The pairs
     * come in the order of `cfHandles`, and each inner stream starts at the first key in ascending order.
     * Each inner stream reads a native iterator that is closed when the outer stream ends, so run the inner
     * streams while the outer stream is running (for example inside `mapZIO` or `flatMap`), not after
     * collecting the outer stream.
     */
    def newIterators(
      cfHandles: List[jrocks.ColumnFamilyHandle]
    ): Stream[Throwable, (jrocks.ColumnFamilyHandle, Stream[Throwable, (Array[Byte], Array[Byte])])] =
      ZStream
        .acquireReleaseWith(ZIO.attempt(db.newIterators(cfHandles.asJava)))(
          its => ZIO.foreach(its.toArray)(it => ZIO.succeed(it.asInstanceOf[RocksIterator].close()))
        )
        .flatMap { its =>
          ZStream.fromIterable {
            cfHandles.zip(its.asScala.toList.map(drainIterator(Direction.Forward, Position.First)))
          }
        }

    /**
     * Writes `value` under `key` in the default ColumnFamily.
     */
    def put(key: Array[Byte], value: Array[Byte]): Task[Unit] =
      ZIO.attempt(db.put(key, value))

    /**
     * Writes `value` under `key` in the ColumnFamily `cfHandle`.
     */
    def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Task[Unit] =
      ZIO.attempt(db.put(cfHandle, key, value))

    /**
     * Applies the writes in `writeBatch` to the database as one batch, with `writeOptions`.
     */
    def write(writeOptions: jrocks.WriteOptions, writeBatch: WriteBatch): Task[Unit] =
      ZIO.attempt(db.write(writeOptions, writeBatch.getUnderlying))
  }

  /**
   * Constructors for `RocksDB.Live`. Each `open` method returns a scoped `RocksDB` that closes the database when
   * the scope closes.
   */
  object Live {

    /**
     * Returns the names of the ColumnFamilies stored in the database at `path`, as raw bytes.
     */
    def listColumnFamilies(options: jrocks.Options, path: String): Task[List[Array[Byte]]] =
      ZIO.attempt(jrocks.RocksDB.listColumnFamilies(options, path).asScala.toList)

    /**
     * Opens the database at `path` with the ColumnFamilies in `cfDescriptors`, and closes it when the scope
     * closes.
     */
    def open(
      options: jrocks.DBOptions,
      path: String,
      cfDescriptors: List[jrocks.ColumnFamilyDescriptor]
    ): ZIO[Scope, Throwable, RocksDB] = {
      val handles = new java.util.ArrayList[jrocks.ColumnFamilyHandle](cfDescriptors.size)
      val db      = ZIO.attempt(jrocks.RocksDB.open(options, path, cfDescriptors.asJava, handles))

      make(db, handles.asScala.toList)
    }

    /**
     * Opens the database at `path` with every ColumnFamily that the database directory lists. `columnFamilyOptions`
     * is used only to list the ColumnFamilies. Each ColumnFamily is opened with default `ColumnFamilyOptions`.
     */
    def openAllColumnFamilies(
      options: jrocks.DBOptions,
      columnFamilyOptions: jrocks.ColumnFamilyOptions,
      path: String
    ): ZIO[Scope, Throwable, RocksDB] =
      for {
        rawColumnFamilies <- listColumnFamilies(new jrocks.Options(options, columnFamilyOptions), path)
        rawColumnFamiliesWithDefault = if (rawColumnFamilies.exists(_.sameElements(rawColumnFamilies)))
          rawColumnFamilies
        else
          rawDefaultColumnFamily :: rawColumnFamilies
        columnFamilies = rawColumnFamilies.map(bytes => new ColumnFamilyDescriptor(bytes))
        live           <- open(options, path, columnFamilies)
      } yield live

    /**
     * Opens the database at `path` with default options, and closes it when the scope closes.
     */
    def open(path: String): ZIO[Scope, Throwable, RocksDB] =
      make(ZIO.attempt(jrocks.RocksDB.open(path)), Nil)

    /**
     * Opens the database at `path` with `options`, and closes it when the scope closes.
     */
    def open(options: jrocks.Options, path: String): ZIO[Scope, Throwable, RocksDB] =
      make(ZIO.attempt(jrocks.RocksDB.open(options, path)), Nil)

    private def make(
      db: Task[jrocks.RocksDB],
      cfHandles: List[jrocks.ColumnFamilyHandle]
    ): ZIO[Scope, Throwable, RocksDB] =
      ZIO.acquireRelease(db)(db => ZIO.attempt(db.closeE()).orDie).map(new Live(_, cfHandles))

    private[rocksdb] val rawDefaultColumnFamily: Array[Byte] = "default".getBytes(StandardCharsets.UTF_8)
  }

  /**
   * Opens the database at the specified path with the specified ColumnFamilies.
   */
  def live(
    options: jrocks.DBOptions,
    path: String,
    cfDescriptors: List[jrocks.ColumnFamilyDescriptor]
  ): ZLayer[Any, Throwable, RocksDB] =
    ZLayer.scoped {
      Live.open(options, path, cfDescriptors)
    }

  /**
   * Opens the default ColumnFamily for the database at the specified path.
   */
  def live(path: String): ZLayer[Any, Throwable, RocksDB] =
    ZLayer.scoped {
      Live.open(path)
    }

  /**
   * Opens the default ColumnFamily for the database at the specified path.
   */
  def live(options: jrocks.Options, path: String): ZLayer[Any, Throwable, RocksDB] =
    ZLayer.scoped {
      Live.open(options, path)
    }

  /**
   * Opens all existing column families for the database at the specified path
   */
  def liveAllColumnFamilies(
    options: jrocks.DBOptions,
    columnFamilyOptions: jrocks.ColumnFamilyOptions,
    path: String
  ): ZLayer[Any, Throwable, RocksDB] =
    ZLayer.scoped(Live.openAllColumnFamilies(options, columnFamilyOptions, path))
}
