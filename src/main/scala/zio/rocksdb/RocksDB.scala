package zio.rocksdb

import org.rocksdb.{ ColumnFamilyDescriptor, ColumnFamilyHandle, ColumnFamilyOptions, RocksIterator }
import org.{ rocksdb => jrocks }
import zio._
import zio.stream.{ Stream, ZStream }

import scala.jdk.CollectionConverters._

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
   * Retrieve a key from the default ColumnFamily in the database.
   */
  def get(key: Array[Byte]): Task[Option[Array[Byte]]]

  /**
   * Retrieve a key from a specific ColumnFamily in the database.
   */
  def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Option[Array[Byte]]]

  /**
   * Retrieves the list of ColumnFamily handles the database was opened with.
   *
   * Caveats:
   * - This list will only be populated if the database was opened with a specific list of
   *   column families.
   * - The list will not be updated if column families are added/removed while the database
   *   is open.
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
   * Scans a specific ColumnFamily in the database and emits the results as a `ZStream`.
   */
  def newIterator(cfHandle: jrocks.ColumnFamilyHandle): Stream[Throwable, (Array[Byte], Array[Byte])]

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
   * Writes a key to a specific ColumnFamily in the database.
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
}

object RocksDB extends Operations[RocksDB] {
  class Live protected (db: jrocks.RocksDB, cfHandles: List[jrocks.ColumnFamilyHandle]) extends RocksDB {

    def createColumnFamily(columnFamilyDescriptor: ColumnFamilyDescriptor): Task[ColumnFamilyHandle] =
      ZIO.attempt(db.createColumnFamily(columnFamilyDescriptor))

    def createColumnFamilies(
      columnFamilyDescriptors: List[ColumnFamilyDescriptor]
    ): Task[List[ColumnFamilyHandle]] =
      ZIO.attempt(db.createColumnFamilies(columnFamilyDescriptors.asJava).asScala.toList)

    def createColumnFamilies(
      columnFamilyOptions: ColumnFamilyOptions,
      columnFamilyNames: List[Array[Byte]]
    ): Task[List[ColumnFamilyHandle]] =
      ZIO.attempt(db.createColumnFamilies(columnFamilyOptions, columnFamilyNames.asJava).asScala.toList)

    def delete(key: Array[Byte]): Task[Unit] =
      ZIO.attempt(db.delete(key))

    def delete(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Unit] =
      ZIO.attempt(db.delete(cfHandle, key))

    def dropColumnFamily(columnFamilyHandle: ColumnFamilyHandle): Task[Unit] =
      ZIO.attempt(db.dropColumnFamily(columnFamilyHandle))

    def dropColumnFamilies(columnFamilyHandles: List[ColumnFamilyHandle]): Task[Unit] =
      ZIO.attempt(db.dropColumnFamilies(columnFamilyHandles.asJava))

    def get(key: Array[Byte]): Task[Option[Array[Byte]]] =
      ZIO.attempt(Option(db.get(key)))

    def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Option[Array[Byte]]] =
      ZIO.attempt(Option(db.get(cfHandle, key)))

    def initialHandles: Task[List[jrocks.ColumnFamilyHandle]] =
      ZIO.succeed(cfHandles)

    def multiGetAsList(keys: List[Array[Byte]]): Task[List[Option[Array[Byte]]]] =
      ZIO.attempt(db.multiGetAsList(keys.asJava).asScala.toList.map(Option(_)))

    def multiGetAsList(
      handles: List[jrocks.ColumnFamilyHandle],
      keys: List[Array[Byte]]
    ): Task[List[Option[Array[Byte]]]] =
      ZIO.attempt {
        db.multiGetAsList(handles.asJava, keys.asJava).asScala.toList.map(Option(_))
      }

    private def drainIterator(it: jrocks.RocksIterator): Stream[Throwable, (Array[Byte], Array[Byte])] =
      ZStream.fromZIO(ZIO.attempt(it.seekToFirst())).drain ++
        ZStream.fromZIO(ZIO.attempt(it.isValid)).flatMap { valid =>
          if (!valid) ZStream.empty
          else
            ZStream.fromZIO(ZIO.attempt(it.key() -> it.value())) ++ ZStream.repeatZIOOption {
              ZIO.attempt {
                it.next()

                if (!it.isValid) ZIO.fail(None)
                else ZIO.succeed(it.key() -> it.value())
              }.mapError(Some(_)).flatten
            }
        }

    def newIterator: Stream[Throwable, (Array[Byte], Array[Byte])] =
      ZStream
        .acquireReleaseWith(ZIO.attempt(db.newIterator()))(it => ZIO.succeed(it.close()))
        .flatMap(drainIterator)

    def getIterator: Task[RocksIterator] = ZIO.attempt(db.newIterator())

    def newIterator(cfHandle: jrocks.ColumnFamilyHandle): Stream[Throwable, (Array[Byte], Array[Byte])] =
      ZStream
        .acquireReleaseWith(ZIO.attempt(db.newIterator(cfHandle)))(it => ZIO.succeed(it.close()))
        .flatMap(drainIterator)

    def newIterators(
      cfHandles: List[jrocks.ColumnFamilyHandle]
    ): Stream[Throwable, (jrocks.ColumnFamilyHandle, Stream[Throwable, (Array[Byte], Array[Byte])])] =
      ZStream
        .acquireReleaseWith(ZIO.attempt(db.newIterators(cfHandles.asJava)))(
          its => UIO.foreach(its.toArray)(it => ZIO.succeed(it.asInstanceOf[RocksIterator].close()))
        )
        .flatMap { its =>
          ZStream.fromIterable {
            cfHandles.zip(its.asScala.toList.map(drainIterator))
          }
        }

    def put(key: Array[Byte], value: Array[Byte]): Task[Unit] =
      ZIO.attempt(db.put(key, value))

    def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Task[Unit] =
      ZIO.attempt(db.put(cfHandle, key, value))
  }

  object Live {

    def listColumnFamilies(options: jrocks.Options, path: String): Task[List[Array[Byte]]] =
      ZIO.attempt(jrocks.RocksDB.listColumnFamilies(options, path).asScala.toList)

    def open(
      options: jrocks.DBOptions,
      path: String,
      cfDescriptors: List[jrocks.ColumnFamilyDescriptor]
    ): ZIO[Scope, Throwable, RocksDB] = {
      val handles = new java.util.ArrayList[jrocks.ColumnFamilyHandle](cfDescriptors.size)
      val db      = ZIO.attempt(jrocks.RocksDB.open(options, path, cfDescriptors.asJava, handles))

      make(db, handles.asScala.toList)
    }

    def open(path: String): ZIO[Scope, Throwable, RocksDB] =
      make(ZIO.attempt(jrocks.RocksDB.open(path)), Nil)

    def open(options: jrocks.Options, path: String): ZIO[Scope, Throwable, RocksDB] =
      make(ZIO.attempt(jrocks.RocksDB.open(options, path)), Nil)

    private def make(
      db: Task[jrocks.RocksDB],
      cfHandles: List[jrocks.ColumnFamilyHandle]
    ): ZIO[Scope, Throwable, RocksDB] =
      ZIO.acquireRelease(db)(db => ZIO.attempt(db.closeE()).orDie).map(new Live(_, cfHandles))
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
}
