package zio

import org.{ rocksdb => jrocks }
import zio.stream.ZStream

package object rocksdb extends RocksDB.Service[RocksDB] {
  def close: URIO[RocksDB, Unit] =
    URIO.accessM(_.rocksDB.close)

  def delete(key: Array[Byte]): RIO[RocksDB, Unit] =
    RIO.accessM(_.rocksDB.delete(key))

  def delete(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): RIO[RocksDB, Unit] =
    RIO.accessM(_.rocksDB.delete(cfHandle, key))

  def get(key: Array[Byte]): RIO[RocksDB, Option[Array[Byte]]] =
    RIO.accessM(_.rocksDB.get(key))

  def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): RIO[RocksDB, Option[Array[Byte]]] =
    RIO.accessM(_.rocksDB.get(cfHandle, key))

  def multiGetAsList(keys: List[Array[Byte]]): RIO[RocksDB, List[Option[Array[Byte]]]] =
    RIO.accessM(_.rocksDB.multiGetAsList(keys))

  def multiGetAsList(
    keysAndHandles: Map[Array[Byte], jrocks.ColumnFamilyHandle]
  ): RIO[RocksDB, List[Option[Array[Byte]]]] =
    RIO.accessM(_.rocksDB.multiGetAsList(keysAndHandles))

  def newIterator: ZStream[RocksDB, Throwable, (Array[Byte], Array[Byte])] =
    ZStream.unwrap(ZIO.access[RocksDB](_.rocksDB.newIterator))

  def newIterator(cfHandle: jrocks.ColumnFamilyHandle): ZStream[RocksDB, Throwable, (Array[Byte], Array[Byte])] =
    ZStream.unwrap(ZIO.access[RocksDB](_.rocksDB.newIterator(cfHandle)))

  def newIterators(
    cfHandles: List[jrocks.ColumnFamilyHandle]
  ): ZStream[RocksDB, Throwable, (jrocks.ColumnFamilyHandle, ZStream[RocksDB, Throwable, (Array[Byte], Array[Byte])])] =
    ZStream.unwrap(ZIO.access[RocksDB](_.rocksDB.newIterators(cfHandles)))

  def put(key: Array[Byte], value: Array[Byte]): RIO[RocksDB, Unit] =
    RIO.accessM(_.rocksDB.put(key, value))

  def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): RIO[RocksDB, Unit] =
    RIO.accessM(_.rocksDB.put(cfHandle, key, value))
}
