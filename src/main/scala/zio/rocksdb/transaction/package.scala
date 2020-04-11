package zio.rocksdb

import org.{ rocksdb => jrocks }
import zio._

package object transaction {
  type Bytes         = Array[Byte]
  type Transaction   = Has[TransactionDB.TransactionService]
  type TransactionDB = Has[TransactionDB.Service]

  object TransactionDB {
    trait TransactionService {

      /**
       * Retrieve a key from the default ColumnFamily in the database.
       */
      def get(readOptions: jrocks.ReadOptions, key: Bytes): Task[Option[Bytes]]

      /**
       * Retrieve a key from the default ColumnFamily in the database and prepare to update.
       */
      def getForUpdate(readOptions: jrocks.ReadOptions, key: Bytes, exclusive: Boolean): Task[Option[Bytes]]

      /**
       * Writes a key to the default ColumnFamily in the database.
       */
      def put(key: Bytes, value: Bytes): Task[Unit]

      /**
       * Delete a key from the default ColumnFamily in the database.
       */
      def delete(key: Bytes): Task[Unit]

      /**
       * Commits all the updates in transaction to the database.
       */
      def commit: Task[Unit]

      /**
       * Closes the current transaction.
       */
      def close: UIO[Unit]

      /**
       * Discard all batched writes in this transaction.
       */
      def rollback: Task[Unit]
    }

    trait Service extends RocksDB.Service {

      /**
       * Creates a new Transaction object.
       */
      def beginTransaction(writeOptions: jrocks.WriteOptions): UIO[TransactionService]
    }
  }

  /**
   * Retrieve a key from the default ColumnFamily in the database.
   */
  def get(readOptions: jrocks.ReadOptions, key: Bytes): RIO[Transaction, Option[Bytes]] =
    ZIO.accessM[Transaction](_.get.get(readOptions, key))

  /**
   * Retrieve a key from the default ColumnFamily in the database.
   */
  def get(key: Bytes): RIO[Transaction, Option[Bytes]] = get(new jrocks.ReadOptions(), key)

  /**
   * Retrieve a key from the default ColumnFamily in the database and prepare to update.
   */
  def getForUpdate(readOptions: jrocks.ReadOptions, key: Bytes, exclusive: Boolean): RIO[Transaction, Option[Bytes]] =
    ZIO.accessM[Transaction](_.get.getForUpdate(readOptions, key, exclusive))

  /**
   * Retrieve a key from the default ColumnFamily in the database and prepare to update.
   */
  def getForUpdate(key: Bytes, exclusive: Boolean): RIO[Transaction, Option[Bytes]] =
    getForUpdate(new jrocks.ReadOptions(), key, exclusive)

  /**
   * Commits all the updates in transaction to the database.
   */
  def commit: RIO[Transaction, Unit] = ZIO.accessM[Transaction](_.get.commit)

  /**
   * Discard all batched writes in this transaction.
   */
  def rollback: RIO[Transaction, Unit] = ZIO.accessM[Transaction](_.get.rollback)

  /**
   * Writes a key to the default ColumnFamily in the database.
   */
  def put(key: Bytes, value: Bytes): RIO[Transaction, Unit] = ZIO.accessM[Transaction](_.get.put(key, value))

  /**
   * Delete a key from the default ColumnFamily in the database.
   */
  def delete(key: Bytes): RIO[Transaction, Unit] = ZIO.accessM[Transaction](_.get.delete(key))

  /**
   * Executes the provided ZIO in one Transaction and commits all the updates in the end.
   */
  def atomically[R <: Has[_], E >: Throwable, A](
    zio: ZIO[Transaction with R, E, A]
  ): ZIO[TransactionDB with R, E, A] = (zio <* commit).provideSomeLayer[TransactionDB with R](LiveTransaction.live)
}
