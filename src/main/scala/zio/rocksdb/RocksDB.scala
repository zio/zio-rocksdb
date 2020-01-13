package zio.rocksdb

// import java.{ util => ju }
import org.{ rocksdb => jrocks }
import zio._
import zio.stream._

// import scala.jdk.CollectionConverters._

trait RocksDB {
  val rocksdb: RocksDB.Service[Any]
}

object RocksDB {
  trait Service[R] {
    def close: URIO[R, Unit]

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
    ): ZStream[R, Throwable, (jrocks.ColumnFamilyHandle, Stream[Throwable, (Array[Byte], Array[Byte])])]

    def put(key: Array[Byte], value: Array[Byte]): RIO[R, Unit]

    def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): RIO[R, Unit]
  }

  trait Live extends RocksDB {
    override val rocksdb: Service[Any] =
      new Service[Any] {
        def close: UIO[Unit] = ???

        def delete(key: Array[Byte]): Task[Unit] = ???

        def delete(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Unit] = ???

        def get(key: Array[Byte]): Task[Option[Array[Byte]]] = ???

        def get(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte]): Task[Option[Array[Byte]]] = ???

        def multiGetAsList(keys: List[Array[Byte]]): Task[List[Option[Array[Byte]]]] = ???

        def multiGetAsList(
          keysAndHandles: Map[Array[Byte], jrocks.ColumnFamilyHandle]
        ): Task[List[Option[Array[Byte]]]] = ???

        def newIterator: Stream[Throwable, (Array[Byte], Array[Byte])] = ???

        def newIterator(cfHandle: jrocks.ColumnFamilyHandle): Stream[Throwable, (Array[Byte], Array[Byte])] = ???

        def newIterators(
          cfHandles: List[jrocks.ColumnFamilyHandle]
        ): Stream[Throwable, (jrocks.ColumnFamilyHandle, Stream[Throwable, (Array[Byte], Array[Byte])])] = ???

        def put(key: Array[Byte], value: Array[Byte]): Task[Unit] = ???

        def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Task[Unit] = ???

      }
  }
}
