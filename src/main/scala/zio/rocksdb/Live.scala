package zio.rocksdb

import java.{ util => ju }

import org.{ rocksdb => jrocks }
import zio._
import zio.stream._

import scala.jdk.CollectionConverters._

class Live protected[rocksdb] (db: jrocks.RocksDB, cfHandles: List[jrocks.ColumnFamilyHandle]) extends RocksDB.Service {
  def delete(key: Array[Byte]): Task[Unit] =
    Task(db.delete(key))

  def delete(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Unit] =
    Task(db.delete(cfHandle, key))

  def get(key: Array[Byte]): Task[Option[Array[Byte]]] =
    Task(Option(db.get(key)))

  def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Option[Array[Byte]]] =
    Task(Option(db.get(cfHandle, key)))

  def initialHandles: Task[List[jrocks.ColumnFamilyHandle]] =
    Task.succeed(cfHandles)

  def multiGetAsList(keys: List[Array[Byte]]): Task[List[Option[Array[Byte]]]] =
    Task(db.multiGetAsList(keys.asJava).asScala.toList.map(Option(_)))

  def multiGetAsList(
    handles: List[jrocks.ColumnFamilyHandle],
    keys: List[Array[Byte]]
  ): Task[List[Option[Array[Byte]]]] =
    Task {
      db.multiGetAsList(handles.asJava, keys.asJava).asScala.toList.map(Option(_))
    }

  private def drainIterator(it: jrocks.RocksIterator): Stream[Throwable, (Array[Byte], Array[Byte])] =
    ZStream.fromEffect(Task(it.seekToFirst())).drain ++
      ZStream.fromEffect(Task(it.isValid())).flatMap { valid =>
        if (!valid) ZStream.empty
        else
          ZStream.fromEffect(Task(it.key() -> it.value())) ++ ZStream.repeatEffectOption {
            Task {
              it.next()

              if (!it.isValid()) ZIO.fail(None)
              else UIO(it.key() -> it.value())
            }.mapError(Some(_)).flatten
          }
      }

  def newIterator: Stream[Throwable, (Array[Byte], Array[Byte])] =
    ZStream
      .bracket(Task(db.newIterator()))(it => UIO(it.close()))
      .flatMap(drainIterator)

  def newIterator(cfHandle: jrocks.ColumnFamilyHandle): Stream[Throwable, (Array[Byte], Array[Byte])] =
    ZStream
      .bracket(Task(db.newIterator(cfHandle)))(it => UIO(it.close()))
      .flatMap(drainIterator)

  def newIterators(
    cfHandles: List[jrocks.ColumnFamilyHandle]
  ): Stream[Throwable, (jrocks.ColumnFamilyHandle, Stream[Throwable, (Array[Byte], Array[Byte])])] =
    ZStream
      .bracket(Task(db.newIterators(cfHandles.asJava)))(its => UIO.foreach(its.asScala)(it => UIO(it.close())))
      .flatMap { its =>
        ZStream.fromIterable {
          cfHandles.zip(its.asScala.toList.map(drainIterator))
        }
      }

  def put(key: Array[Byte], value: Array[Byte]): Task[Unit] =
    Task(db.put(key, value))

  def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Task[Unit] =
    Task(db.put(cfHandle, key, value))
}

object Live {
  def listColumnFamilies(options: jrocks.Options, path: String): Task[List[Array[Byte]]] =
    Task(jrocks.RocksDB.listColumnFamilies(options, path).asScala.toList)

  def open(
    options: jrocks.DBOptions,
    path: String,
    cfDescriptors: List[jrocks.ColumnFamilyDescriptor]
  ): Managed[Throwable, RocksDB.Service] = {
    val handles = new ju.ArrayList[jrocks.ColumnFamilyHandle](cfDescriptors.size)
    val db      = Task(jrocks.RocksDB.open(options, path, cfDescriptors.asJava, handles))

    make(db, handles.asScala.toList)
  }

  def open(path: String): Managed[Throwable, RocksDB.Service] =
    make(Task(jrocks.RocksDB.open(path)), Nil)

  def open(options: jrocks.Options, path: String): Managed[Throwable, RocksDB.Service] =
    make(Task(jrocks.RocksDB.open(options, path)), Nil)

  private def make(
    db: Task[jrocks.RocksDB],
    cfHandles: List[jrocks.ColumnFamilyHandle]
  ): Managed[Throwable, RocksDB.Service] =
    db.toManaged(db => Task(db.closeE()).orDie).map(new Live(_, cfHandles))
}
