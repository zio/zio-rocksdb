package zio.rocksdb

import org.{ rocksdb => jrocks }
import zio._

package object transaction {
  type Bytes         = Array[Byte]
  type Transaction   = Has[RocksDB.TransactionService]
  type TransactionDB = Has[RocksDB.TransactionDBService]

  object RocksDB {
    trait TransactionService {
      def get(readOptions: jrocks.ReadOptions, key: Bytes): Task[Option[Bytes]]
      def getForUpdate(readOptions: jrocks.ReadOptions, key: Bytes, exclusive: Boolean): Task[Option[Bytes]]
      def put(key: Bytes, value: Bytes): Task[Unit]
      def delete(key: Bytes): Task[Unit]
      def commit: Task[Unit]
    }

    trait TransactionDBService {
      def beginTransaction(writeOptions: jrocks.WriteOptions): UIO[TransactionService]
    }

    def transaction[R <: Has[_], E >: Throwable, A](
      zio: ZIO[Transaction with R, E, A]
    ): ZIO[TransactionDB with R, E, A] = zio.provideSomeLayer[TransactionDB with R](ZTransaction.live)
  }

  // Helper Utilities
  def get(readOptions: jrocks.ReadOptions, key: Bytes): RIO[Transaction, Option[Bytes]] =
    ZIO.accessM[Transaction](_.get.get(readOptions, key))
  def get(key: Bytes): RIO[Transaction, Option[Bytes]] = get(new jrocks.ReadOptions(), key)

  def getForUpdate(readOptions: jrocks.ReadOptions, key: Bytes, exclusive: Boolean): RIO[Transaction, Option[Bytes]] =
    ZIO.accessM[Transaction](_.get.getForUpdate(readOptions, key, exclusive))
  def getForUpdate(key: Bytes, exclusive: Boolean): RIO[Transaction, Option[Bytes]] =
    getForUpdate(new jrocks.ReadOptions(), key, exclusive)

  def put(key: Bytes, value: Bytes): RIO[Transaction, Unit] = ZIO.accessM[Transaction](_.get.put(key, value))
  def delete(key: Bytes): RIO[Transaction, Unit]            = ZIO.accessM[Transaction](_.get.delete(key))
}
