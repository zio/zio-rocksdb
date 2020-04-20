package zio.rocksdb.service

import org.{ rocksdb => jrocks }
import zio.{ Has, ZIO, ZManaged }
import zio.rocksdb.Atomically

trait TransactionDB extends RocksDB {
  def beginTransaction(writeOptions: jrocks.WriteOptions): ZManaged[Any, Nothing, Transaction]
  def beginTransaction: ZManaged[Any, Nothing, Transaction] = beginTransaction(new jrocks.WriteOptions())
  def atomically[R <: Has[_], E >: Throwable, A](writeOptions: jrocks.WriteOptions)(
    zio: ZIO[Has[Transaction] with R, E, A]
  )(implicit A: Atomically.TransactionWithSomething): ZIO[R, E, A]
  def atomically[R <: Has[_], E >: Throwable, A](zio: ZIO[Has[Transaction] with R, E, A])(
    implicit A: Atomically.TransactionWithSomething
  ): ZIO[R, E, A] = atomically[R, E, A](new jrocks.WriteOptions())(zio)

  def atomically[E >: Throwable, A](writeOptions: jrocks.WriteOptions)(zio: ZIO[Has[Transaction], E, A])(
    implicit A: Atomically.TransactionOnly
  ): ZIO[Any, E, A]

  def atomically[E >: Throwable, A](zio: ZIO[Has[Transaction], E, A])(
    implicit A: Atomically.TransactionOnly
  ): ZIO[Any, E, A] = atomically[E, A](new jrocks.WriteOptions())(zio)
}
