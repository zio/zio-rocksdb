package zio

import org.{ rocksdb => jrocks }
import zio.stream.{ Stream, ZStream }

package object rocksdb {
  type Bytes = Chunk[Byte]

  type RocksDB = Has[RocksDB.Service]

  object RocksDB {
    trait Service {

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
    }

    /**
     * Opens the database at the specified path with the specified ColumnFamilies.
     */
    def live(
      options: jrocks.DBOptions,
      path: String,
      cfDescriptors: List[jrocks.ColumnFamilyDescriptor]
    ): ZLayer.NoDeps[Throwable, RocksDB] =
      ZLayer.fromManaged(Live.open(options, path, cfDescriptors))

    /**
     * Opens the default ColumnFamily for the database at the specified path.
     */
    def live(path: String): ZLayer.NoDeps[Throwable, RocksDB] =
      ZLayer.fromManaged(Live.open(path))

    /**
     * Opens the default ColumnFamily for the database at the specified path.
     */
    def live(options: jrocks.Options, path: String): ZLayer.NoDeps[Throwable, RocksDB] =
      ZLayer.fromManaged(Live.open(options, path))
  }

  def delete(key: Array[Byte]): RIO[RocksDB, Unit] =
    RIO.accessM(_.get.delete(key))

  def delete(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): RIO[RocksDB, Unit] =
    RIO.accessM(_.get.delete(cfHandle, key))

  def get(key: Array[Byte]): RIO[RocksDB, Option[Array[Byte]]] =
    RIO.accessM(_.get.get(key))

  def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): RIO[RocksDB, Option[Array[Byte]]] =
    RIO.accessM(_.get.get(cfHandle, key))

  def multiGetAsList(keys: List[Array[Byte]]): RIO[RocksDB, List[Option[Array[Byte]]]] =
    RIO.accessM(_.get.multiGetAsList(keys))

  def multiGetAsList(
    handles: List[jrocks.ColumnFamilyHandle],
    keys: List[Array[Byte]]
  ): RIO[RocksDB, List[Option[Array[Byte]]]] =
    RIO.accessM(_.get.multiGetAsList(handles, keys))

  def newIterator: ZStream[RocksDB, Throwable, (Array[Byte], Array[Byte])] =
    ZStream.unwrap(ZIO.access[RocksDB](_.get.newIterator))

  def newIterator(cfHandle: jrocks.ColumnFamilyHandle): ZStream[RocksDB, Throwable, (Array[Byte], Array[Byte])] =
    ZStream.unwrap(ZIO.access[RocksDB](_.get.newIterator(cfHandle)))

  def newIterators(
    cfHandles: List[jrocks.ColumnFamilyHandle]
  ): ZStream[RocksDB, Throwable, (jrocks.ColumnFamilyHandle, ZStream[RocksDB, Throwable, (Array[Byte], Array[Byte])])] =
    ZStream.unwrap(ZIO.access[RocksDB](_.get.newIterators(cfHandles)))

  def put(key: Array[Byte], value: Array[Byte]): RIO[RocksDB, Unit] =
    RIO.accessM(_.get.put(key, value))

  def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): RIO[RocksDB, Unit] =
    RIO.accessM(_.get.put(cfHandle, key, value))
}
