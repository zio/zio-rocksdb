package zio.rocksdb.service

import org.{ rocksdb => jrocks }
import zio.{ Has, UIO, ZIO }
import zio.rocksdb.Atomically

trait TransactionDB extends RocksDB {
  def beginTransaction(writeOptions: jrocks.WriteOptions): UIO[Transaction]
  def beginTransaction: UIO[Transaction] = beginTransaction(new jrocks.WriteOptions())
  def atomically[R <: Has[_], E >: Throwable, A](zio: ZIO[Has[Transaction] with R, E, A])(
    implicit A: Atomically.TransactionWithSomething
  ): ZIO[R, E, A]
  def atomically[E >: Throwable, A](zio: ZIO[Has[Transaction], E, A])(
    implicit A: Atomically.TransactionOnly
  ): ZIO[Any, E, A]
}
