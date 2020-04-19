package zio.rocksdb.service

import org.{ rocksdb => jrocks }
import zio.{ Has, UIO, ZIO }

trait TransactionDB extends RocksDB {
  def beginTransaction(writeOptions: jrocks.WriteOptions): UIO[Transaction]
  def beginTransaction: UIO[Transaction]
  def atomically[R <: Has[_], E >: Throwable, A](that: ZIO[zio.rocksdb.Transaction with R, E, A]): ZIO[R, E, A]
}
