package zio.rocksdb

import java.{ util => ju }
import org.{ rocksdb => jrocks }
import zio._
import zio.stream._

import scala.jdk.CollectionConverters._

trait RocksDB {
  val rocksDB: RocksDB.Service[Any]
}

object RocksDB {
  trait Service[R] {
    def delete(key: Array[Byte]): RIO[R, Unit]

    def delete(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): RIO[R, Unit]

    def get(key: Array[Byte]): RIO[R, Option[Array[Byte]]]

    def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): RIO[R, Option[Array[Byte]]]

    def multiGetAsList(keys: List[Array[Byte]]): RIO[R, List[Option[Array[Byte]]]]

    def multiGetAsList(keysAndHandles: Map[Array[Byte], jrocks.ColumnFamilyHandle]): RIO[R, List[Option[Array[Byte]]]]

    def newIterator: ZStream[R, Throwable, (Array[Byte], Array[Byte])]

    def newIterator(cfHandle: jrocks.ColumnFamilyHandle): ZStream[R, Throwable, (Array[Byte], Array[Byte])]

    def newIterators(
      cfHandles: List[jrocks.ColumnFamilyHandle]
    ): ZStream[R, Throwable, (jrocks.ColumnFamilyHandle, ZStream[R, Throwable, (Array[Byte], Array[Byte])])]

    def put(key: Array[Byte], value: Array[Byte]): RIO[R, Unit]

    def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): RIO[R, Unit]
  }

  final class Live private (db: jrocks.RocksDB) extends Service[Any] {
    def delete(key: Array[Byte]): Task[Unit] =
      Task(db.delete(key))

    def delete(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Unit] =
      Task(db.delete(cfHandle, key))

    def get(key: Array[Byte]): Task[Option[Array[Byte]]] =
      Task(Option(db.get(key)))

    def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Option[Array[Byte]]] =
      Task(Option(db.get(cfHandle, key)))

    def multiGetAsList(keys: List[Array[Byte]]): Task[List[Option[Array[Byte]]]] =
      Task(db.multiGetAsList(keys.asJava).asScala.toList.map(Option(_)))

    def multiGetAsList(
      keysAndHandles: Map[Array[Byte], jrocks.ColumnFamilyHandle]
    ): Task[List[Option[Array[Byte]]]] =
      Task {
        val (keys, handles) = keysAndHandles.toList.unzip
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
    ): Managed[Throwable, (RocksDB, List[jrocks.ColumnFamilyHandle])] = {
      val handles = new ju.ArrayList[jrocks.ColumnFamilyHandle](cfDescriptors.size)
      val db      = Task(jrocks.RocksDB.open(options, path, cfDescriptors.asJava, handles))

      make(db).map(_ -> handles.asScala.toList)
    }

    def open(path: String): Managed[Throwable, RocksDB] =
      make(Task(jrocks.RocksDB.open(path)))

    def open(options: jrocks.Options, path: String): Managed[Throwable, RocksDB] =
      make(Task(jrocks.RocksDB.open(options, path)))

    private def make(db: Task[jrocks.RocksDB]): Managed[Throwable, RocksDB] =
      db.toManaged(db => Task(db.closeE()).orDie).map { db =>
        new RocksDB {
          val rocksDB: Service[Any] = new RocksDB.Live(db)
        }
      }
  }
}
