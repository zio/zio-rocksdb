package zio.rocksdb

import java.{ util => ju }

import org.{ rocksdb => jrocks }
import zio._
import zio.stream.{ Stream, ZStream }

import scala.jdk.CollectionConverters._

object RocksDB extends Operations[RocksDB, service.RocksDB] {
  class Live protected (db: jrocks.RocksDB, cfHandles: List[jrocks.ColumnFamilyHandle]) extends service.RocksDB {
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
      ZStream.fromZIO(Task(it.seekToFirst())).drain ++
        ZStream.fromZIO(Task(it.isValid)).flatMap { valid =>
          if (!valid) ZStream.empty
          else
            ZStream.fromZIO(Task(it.key() -> it.value())) ++ ZStream.repeatZIOOption {
              Task {
                it.next()

                if (!it.isValid) ZIO.fail(None)
                else UIO(it.key() -> it.value())
              }.mapError(Some(_)).flatten
            }
        }

    def newIterator: Stream[Throwable, (Array[Byte], Array[Byte])] =
      ZStream
        .acquireReleaseWith(Task(db.newIterator()))(it => UIO(it.close()))
        .flatMap(drainIterator)

    def newIterator(cfHandle: jrocks.ColumnFamilyHandle): Stream[Throwable, (Array[Byte], Array[Byte])] =
      ZStream
        .acquireReleaseWith(Task(db.newIterator(cfHandle)))(it => UIO(it.close()))
        .flatMap(drainIterator)

    def newIterators(
      cfHandles: List[jrocks.ColumnFamilyHandle]
    ): Stream[Throwable, (jrocks.ColumnFamilyHandle, Stream[Throwable, (Array[Byte], Array[Byte])])] =
      ZStream
        .acquireReleaseWith(
          Task(db.newIterators(cfHandles.asJava))
        )(
          iterators => UIO.foreach(iterators.asScala.toSeq)(iterator => UIO(iterator.close()))
        )
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
    ): Managed[Throwable, service.RocksDB] = {
      val handles = new ju.ArrayList[jrocks.ColumnFamilyHandle](cfDescriptors.size)
      val db      = Task(jrocks.RocksDB.open(options, path, cfDescriptors.asJava, handles))

      make(db, handles.asScala.toList)
    }

    def open(path: String): Managed[Throwable, service.RocksDB] =
      make(Task(jrocks.RocksDB.open(path)), Nil)

    def open(options: jrocks.Options, path: String): Managed[Throwable, service.RocksDB] =
      make(Task(jrocks.RocksDB.open(options, path)), Nil)

    private def make(
      db: Task[jrocks.RocksDB],
      cfHandles: List[jrocks.ColumnFamilyHandle]
    ): Managed[Throwable, service.RocksDB] =
      ZManaged
        .acquireReleaseWith(db)(
          db => ZIO.succeed(db.closeE())
        )
        .map(new Live(_, cfHandles))
  }

  /**
   * Opens the database at the specified path with the specified ColumnFamilies.
   */
  def live(
    options: jrocks.DBOptions,
    path: String,
    cfDescriptors: List[jrocks.ColumnFamilyDescriptor]
  ): Layer[Throwable, RocksDB] =
    ZLayer.fromManaged(Live.open(options, path, cfDescriptors))

  /**
   * Opens the default ColumnFamily for the database at the specified path.
   */
  def live(path: String): Layer[Throwable, RocksDB] =
    ZLayer.fromManaged(Live.open(path))

  /**
   * Opens the default ColumnFamily for the database at the specified path.
   */
  def live(options: jrocks.Options, path: String): Layer[Throwable, RocksDB] =
    ZLayer.fromManaged(Live.open(options, path))
}
