package zio.rocksdb
import org.rocksdb.ColumnFamilyHandle
import org.{ rocksdb => jrocks }
import zio.{ Task, TaskManaged }

final class WriteBatch private (batch: jrocks.WriteBatch) {
  def put(key: Array[Byte], value: Array[Byte]): Task[Unit] = Task(batch.put(key, value))

  def put(cfHandle: ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Task[Unit] =
    Task(batch.put(cfHandle, key, value))

  private[rocksdb] def getUnderlying: jrocks.WriteBatch = batch
}

object WriteBatch {
  def open: TaskManaged[WriteBatch] = make(new jrocks.WriteBatch())

  def open(reservedBytes: Int): TaskManaged[WriteBatch] = make(new jrocks.WriteBatch(reservedBytes))

  def open(serialized: Array[Byte]): TaskManaged[WriteBatch] = make(new jrocks.WriteBatch(serialized))

  private def make(batch: jrocks.WriteBatch): TaskManaged[WriteBatch] =
    Task(batch).toManaged(batch => Task(batch.close()).orDie).map(new WriteBatch(_))
}
