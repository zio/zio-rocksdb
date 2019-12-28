package zio.rocksdb

import java.{ util => ju }
import org.{ rocksdb => jrocks }
import zio._
import zio.stream._

import scala.collection.JavaConverters._

case class RocksDB(db: jrocks.RocksDB, cfHandles: List[jrocks.ColumnFamilyHandle]) {
  def delete(key: Array[Byte]) =
    Task(db.delete(key))

  def delete(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]) =
    Task(db.delete(cfHandle, key))

  def get(key: Array[Byte]) =
    Task(Option(db.get(key)))

  def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]) =
    Task(Option(db.get(cfHandle, key)))

  def multiGetAsList(keys: List[Array[Byte]]) =
    Task(db.multiGetAsList(keys.asJava).asScala.toList.map(Option(_)))

  def multiGetAsList(keysAndHandles: Map[Array[Byte], jrocks.ColumnFamilyHandle]) =
    Task {
      val (keys, handles) = keysAndHandles.toList.unzip
      db.multiGetAsList(handles.asJava, keys.asJava).asScala.toList.map(Option(_))
    }

  def newIterator: Stream[Throwable, (Array[Byte], Array[Byte])] =
    ZStream
      .bracket(Task(db.newIterator()))(it => UIO(it.close()))
      .flatMap { it =>
        ZStream.fromEffect(Task(it.seekToFirst())).drain ++
          convertIterator(it)
      }

  private def convertIterator(it: jrocks.RocksIterator): Stream[Throwable, (Array[Byte], Array[Byte])] =
    ZStream.fromPull {
      Task {
        if (!it.isValid()) ZIO.fail(None)
        else
          UIO {
            it.next()
            it.key() -> it.value()
          }
      }.mapError(Some(_)).flatten
    }

  def newIterator(cfHandle: jrocks.ColumnFamilyHandle): Stream[Throwable, (Array[Byte], Array[Byte])] =
    ZStream
      .bracket(Task(db.newIterator(cfHandle)))(it => UIO(it.close()))
      .flatMap { it =>
        ZStream.fromEffect(Task(it.seekToFirst())).drain ++
          convertIterator(it)
      }

  def newIterators(
    cfHandles: List[jrocks.ColumnFamilyHandle]
  ): Stream[Throwable, (jrocks.ColumnFamilyHandle, Stream[Throwable, (Array[Byte], Array[Byte])])] =
    ZStream
      .bracket(Task(db.newIterators(cfHandles.asJava)))(its => UIO.foreach(its.asScala)(it => UIO(it.close())))
      .flatMap { its =>
        ZStream.fromIterable {
          cfHandles.zip(its.asScala.toList.map { it =>
            ZStream.fromEffect(Task(it.seekToFirst())).drain ++
              convertIterator(it)
          })
        }
      }

  def put(key: Array[Byte], value: Array[Byte]) =
    Task(db.put(key, value))

  def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]) =
    Task(db.put(cfHandle, key, value))

  def close: UIO[Unit] = UIO(db.closeE())
}

object RocksDB {
  def make(
    options: jrocks.DBOptions,
    path: String,
    cfDescriptors: List[jrocks.ColumnFamilyDescriptor]
  ): Managed[Throwable, RocksDB] =
    Task {
      val handles = new ju.ArrayList[jrocks.ColumnFamilyHandle](cfDescriptors.size)
      val db      = jrocks.RocksDB.open(options, path, cfDescriptors.asJava, handles)

      new RocksDB(db, handles.asScala.toList)
    }.toManaged(_.close)

  def listColumnFamilies(options: jrocks.Options, path: String) =
    Task(jrocks.RocksDB.listColumnFamilies(options, path))
}
