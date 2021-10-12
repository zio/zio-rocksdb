package zio.rocksdb

import org.{ rocksdb => jrocks }
import zio.stream.ZStream
import zio.{ Has, RIO, Tag }

abstract class Operations[R <: Has[S], S <: service.RocksDB](implicit tagged: Tag[S]) {
  private val db: RIO[R, S]                                                       = RIO.access[R](_.get)
  def delete(key: Array[Byte]): RIO[R, Unit]                                      = db flatMap (_.delete(key))
  def delete(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): RIO[R, Unit] = db flatMap (_.delete(cfHandle, key))
  def get(key: Array[Byte]): RIO[R, Option[Array[Byte]]]                          = db flatMap (_.get(key))
  def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): RIO[R, Option[Array[Byte]]] =
    db flatMap (_.get(cfHandle, key))
  def multiGetAsList(keys: List[Array[Byte]]): RIO[R, List[Option[Array[Byte]]]] = db flatMap (_.multiGetAsList(keys))
  def multiGetAsList(
    handles: List[jrocks.ColumnFamilyHandle],
    keys: List[Array[Byte]]
  ): RIO[R, List[Option[Array[Byte]]]] =
    db flatMap (_.multiGetAsList(handles, keys))
  def newIterator: ZStream[R, Throwable, (Array[Byte], Array[Byte])] = ZStream.unwrap(db map (_.newIterator))
  def newIterator(cfHandle: jrocks.ColumnFamilyHandle): ZStream[R, Throwable, (Array[Byte], Array[Byte])] =
    ZStream.unwrap(db map (_.newIterator(cfHandle)))
  def newIterators(
    cfHandles: List[jrocks.ColumnFamilyHandle]
  ): ZStream[R, Throwable, (jrocks.ColumnFamilyHandle, ZStream[R, Throwable, (Array[Byte], Array[Byte])])] =
    ZStream.unwrap(db map (_.newIterators(cfHandles)))
  def put(key: Array[Byte], value: Array[Byte]): RIO[R, Unit] = db flatMap (_.put(key, value))
  def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): RIO[R, Unit] =
    db flatMap (_.put(cfHandle, key, value))
}
