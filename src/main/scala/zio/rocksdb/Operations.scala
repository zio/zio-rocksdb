package zio.rocksdb

import org.rocksdb.{ ColumnFamilyDescriptor, ColumnFamilyHandle, ColumnFamilyOptions }
import org.{ rocksdb => jrocks }
import zio.stream.ZStream
import zio.{ RIO, Tag, ZIO }

abstract class Operations[R <: RocksDB](implicit tagged: Tag[R]) {
  private val db: RIO[R, R] = ZIO.service[R]

  def delete(key: Array[Byte]): RIO[R, Unit] = db.flatMap(_.delete(key))

  def delete(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): RIO[R, Unit] = db.flatMap(_.delete(cfHandle, key))

  def get(key: Array[Byte]): RIO[R, Option[Array[Byte]]] = db.flatMap(_.get(key))

  def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): RIO[R, Option[Array[Byte]]] =
    db.flatMap(_.get(cfHandle, key))

  def multiGetAsList(keys: List[Array[Byte]]): RIO[R, List[Option[Array[Byte]]]] = db.flatMap(_.multiGetAsList(keys))

  def multiGetAsList(
    handles: List[jrocks.ColumnFamilyHandle],
    keys: List[Array[Byte]]
  ): RIO[R, List[Option[Array[Byte]]]] =
    db.flatMap(_.multiGetAsList(handles, keys))

  def newIterator: ZStream[R, Throwable, (Array[Byte], Array[Byte])] = ZStream.unwrap(db map (_.newIterator))

  def newIterator(cfHandle: jrocks.ColumnFamilyHandle): ZStream[R, Throwable, (Array[Byte], Array[Byte])] =
    ZStream.unwrap(db map (_.newIterator(cfHandle)))

  def newIterators(
    cfHandles: List[jrocks.ColumnFamilyHandle]
  ): ZStream[R, Throwable, (jrocks.ColumnFamilyHandle, ZStream[R, Throwable, (Array[Byte], Array[Byte])])] =
    ZStream.unwrap(db map (_.newIterators(cfHandles)))

  def put(key: Array[Byte], value: Array[Byte]): RIO[R, Unit] = db.flatMap(_.put(key, value))

  def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): RIO[R, Unit] =
    db.flatMap(_.put(cfHandle, key, value))

  def createColumnFamily(columnFamilyDescriptor: ColumnFamilyDescriptor): ZIO[R, Throwable, ColumnFamilyHandle] =
    db.flatMap(_.createColumnFamily(columnFamilyDescriptor))

  def createColumnFamilies(
    columnFamilyDescriptors: List[ColumnFamilyDescriptor]
  ): ZIO[R, Throwable, List[ColumnFamilyHandle]] = db.flatMap(_.createColumnFamilies(columnFamilyDescriptors))

  def createColumnFamilies(
    columnFamilyOptions: ColumnFamilyOptions,
    columnFamilyNames: List[Array[Byte]]
  ): ZIO[R, Throwable, List[ColumnFamilyHandle]] =
    db.flatMap(_.createColumnFamilies(columnFamilyOptions, columnFamilyNames))

  def dropColumnFamily(columnFamilyHandle: ColumnFamilyHandle): ZIO[R, Throwable, Unit] =
    db.flatMap(_.dropColumnFamily(columnFamilyHandle))

  def dropColumnFamilies(columnFamilyHandles: List[ColumnFamilyHandle]): ZIO[R, Throwable, Unit] =
    db.flatMap(_.dropColumnFamilies(columnFamilyHandles))
}
