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
       * Retrieve a key from the default ColumnFamily in the database and prepare to updates
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

    trait Service {
      def beginTransaction(writeOptions: jrocks.WriteOptions): UIO[TransactionService]
    }
  }

  // Helper Utilities
  def get(readOptions: jrocks.ReadOptions, key: Bytes): RIO[Transaction, Option[Bytes]] =
    ZIO.accessM[Transaction](_.get.get(readOptions, key))
  def get(key: Bytes): RIO[Transaction, Option[Bytes]] = get(new jrocks.ReadOptions(), key)
  def getForUpdate(readOptions: jrocks.ReadOptions, key: Bytes, exclusive: Boolean): RIO[Transaction, Option[Bytes]] =
    ZIO.accessM[Transaction](_.get.getForUpdate(readOptions, key, exclusive))
  def getForUpdate(key: Bytes, exclusive: Boolean): RIO[Transaction, Option[Bytes]] =
    getForUpdate(new jrocks.ReadOptions(), key, exclusive)
  def commit: RIO[Transaction, Unit]                        = ZIO.accessM[Transaction](_.get.commit)
  def rollback: RIO[Transaction, Unit]                      = ZIO.accessM[Transaction](_.get.rollback)
  def put(key: Bytes, value: Bytes): RIO[Transaction, Unit] = ZIO.accessM[Transaction](_.get.put(key, value))
  def delete(key: Bytes): RIO[Transaction, Unit]            = ZIO.accessM[Transaction](_.get.delete(key))
  def atomically[R <: Has[_], E >: Throwable, A](
    zio: ZIO[Transaction with R, E, A]
  ): ZIO[TransactionDB with R, E, A] = (zio <* commit).provideSomeLayer[TransactionDB with R](LiveTransaction.live)
}
