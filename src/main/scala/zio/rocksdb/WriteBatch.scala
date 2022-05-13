package zio.rocksdb
import org.rocksdb.ColumnFamilyHandle
import org.{ rocksdb => jrocks }
import zio.{ Task, TaskManaged }

object WriteBatch {
  final class Live private (batch: jrocks.WriteBatch) extends service.WriteBatch {
    def put(key: Array[Byte], value: Array[Byte]): Task[Unit] = Task(batch.put(key, value))

    def put(cfHandle: ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Task[Unit] =
      Task(batch.put(cfHandle, key, value))

    private[rocksdb] def getUnderlying: jrocks.WriteBatch = batch
  }

  object Live {
    def open(): TaskManaged[Live] = make(new jrocks.WriteBatch())

    def open(reservedBytes: Int): TaskManaged[Live] = make(new jrocks.WriteBatch(reservedBytes))

    def open(serialized: Array[Byte]): TaskManaged[Live] = make(new jrocks.WriteBatch(serialized))

    private def make(batch: jrocks.WriteBatch): TaskManaged[Live] =
      Task(batch).toManaged(batch => Task(batch.close()).orDie).map(new Live(_))
  }
}
