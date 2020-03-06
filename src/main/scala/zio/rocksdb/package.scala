package zio

import org.{ rocksdb => jrocks }
import zio.stream.{ Stream, ZStream }

package object rocksdb {
  type Bytes = Chunk[Byte]

  type RocksDB = Has[RocksDB.Service]

  object RocksDB {
    trait Service {
      def delete(key: Array[Byte]): Task[Unit]

      def delete(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Unit]

      def get(key: Array[Byte]): Task[Option[Array[Byte]]]

      def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Option[Array[Byte]]]

      def multiGetAsList(keys: List[Array[Byte]]): Task[List[Option[Array[Byte]]]]

      def multiGetAsList(keysAndHandles: Map[Array[Byte], jrocks.ColumnFamilyHandle]): Task[List[Option[Array[Byte]]]]

      def newIterator: Stream[Throwable, (Array[Byte], Array[Byte])]

      def newIterator(cfHandle: jrocks.ColumnFamilyHandle): Stream[Throwable, (Array[Byte], Array[Byte])]

      def newIterators(
        cfHandles: List[jrocks.ColumnFamilyHandle]
      ): Stream[Throwable, (jrocks.ColumnFamilyHandle, Stream[Throwable, (Array[Byte], Array[Byte])])]

      def put(key: Array[Byte], value: Array[Byte]): Task[Unit]

      def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Task[Unit]
    }

    def live(
      options: jrocks.DBOptions,
      path: String,
      cfDescriptors: List[jrocks.ColumnFamilyDescriptor]
    ): ZLayer.NoDeps[Throwable, Has[(RocksDB.Service, List[jrocks.ColumnFamilyHandle])]] =
      ZLayer.fromManaged(Live.open(options, path, cfDescriptors))

    def live(path: String): ZLayer.NoDeps[Throwable, RocksDB] =
      ZLayer.fromManaged(Live.open(path))

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
    keysAndHandles: Map[Array[Byte], jrocks.ColumnFamilyHandle]
  ): RIO[RocksDB, List[Option[Array[Byte]]]] =
    RIO.accessM(_.get.multiGetAsList(keysAndHandles))

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
