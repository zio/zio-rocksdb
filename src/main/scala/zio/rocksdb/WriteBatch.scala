package zio.rocksdb
import org.rocksdb.ColumnFamilyHandle
import org.{ rocksdb => jrocks }
import zio.{ Scope, Task, ZIO }

/**
 * A batch of writes that `RocksDB.write` applies together. Nothing is written until the batch is passed to
 * `RocksDB.write`. Open a batch with `WriteBatch.open`, which closes it when its scope closes.
 */
final class WriteBatch private (batch: jrocks.WriteBatch) {

  /**
   * Adds a write of `value` under `key` in the default ColumnFamily to the batch.
   */
  def put(key: Array[Byte], value: Array[Byte]): Task[Unit] =
    ZIO.attempt(batch.put(key, value))

  /**
   * Adds a write of `value` under `key` in the ColumnFamily `cfHandle` to the batch.
   */
  def put(cfHandle: ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Task[Unit] =
    ZIO.attempt(batch.put(cfHandle, key, value))

  private[rocksdb] def getUnderlying: jrocks.WriteBatch = batch
}

/**
 * Constructors for `WriteBatch`.
 */
object WriteBatch {

  /**
   * Opens an empty batch, scoped to the enclosing `Scope`.
   */
  def open: ZIO[Scope, Throwable, WriteBatch] = make(new jrocks.WriteBatch())

  /**
   * Opens an empty batch with `reservedBytes` of memory reserved for its contents, scoped to the enclosing
   * `Scope`.
   */
  def open(reservedBytes: Int): ZIO[Scope, Throwable, WriteBatch] = make(new jrocks.WriteBatch(reservedBytes))

  /**
   * Opens a batch from `serialized`, which holds the batch in the format of RocksDB's `WriteBatch`, scoped to
   * the enclosing `Scope`.
   */
  def open(serialized: Array[Byte]): ZIO[Scope, Throwable, WriteBatch] = make(new jrocks.WriteBatch(serialized))

  private def make(batch: jrocks.WriteBatch): ZIO[Scope, Throwable, WriteBatch] =
    ZIO.acquireRelease(ZIO.succeed(batch))(batch => ZIO.attempt(batch.close()).orDie).map(new WriteBatch(_))
}
