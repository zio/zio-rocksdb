package zio.rocksdb
import org.rocksdb.ColumnFamilyHandle
import org.{ rocksdb => jrocks }
import zio.{ Scope, Task, ZIO }

final class WriteBatch private (batch: jrocks.WriteBatch) {
  def put(key: Array[Byte], value: Array[Byte]): Task[Unit] =
    ZIO.attempt(batch.put(key, value))

  def put(cfHandle: ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Task[Unit] =
    ZIO.attempt(batch.put(cfHandle, key, value))

  private[rocksdb] def getUnderlying: jrocks.WriteBatch = batch
}

object WriteBatch {
  def open: ZIO[Scope, Throwable, WriteBatch] = make(new jrocks.WriteBatch())

  def open(reservedBytes: Int): ZIO[Scope, Throwable, WriteBatch] = make(new jrocks.WriteBatch(reservedBytes))

  def open(serialized: Array[Byte]): ZIO[Scope, Throwable, WriteBatch] = make(new jrocks.WriteBatch(serialized))

  private def make(batch: jrocks.WriteBatch): ZIO[Scope, Throwable, WriteBatch] =
    ZIO.acquireRelease(ZIO.succeed(batch))(batch => ZIO.attempt(batch.close()).orDie).map(new WriteBatch(_))
}
