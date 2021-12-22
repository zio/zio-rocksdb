package zio.rocksdb.service

import org.{ rocksdb => jrocks }
import zio.{ ZIO, ZManaged }
import zio.rocksdb.Atomically

trait TransactionDB extends RocksDB {

  /**
   * Creates a managed instance of `service.Transaction` using the provided `WriteOptions`.
   */
  def beginTransaction(writeOptions: jrocks.WriteOptions): ZManaged[Any, Throwable, Transaction]

  /**
   * Creates a managed instance of `service.Transaction`.
   */
  def beginTransaction: ZManaged[Any, Throwable, Transaction] = beginTransaction(new jrocks.WriteOptions())

  /**
   * Executes the provided zio program in a single transaction.
   */
  def atomically[R, E >: Throwable, A](writeOptions: jrocks.WriteOptions)(
    zio: ZIO[Transaction with R, E, A]
  )(implicit A: Atomically.TransactionWithSomething): ZIO[R, E, A]

  /**
   * Executes the provided zio program in a single transaction.
   */
  def atomically[R, E >: Throwable, A](zio: ZIO[Transaction with R, E, A])(
    implicit A: Atomically.TransactionWithSomething
  ): ZIO[R, E, A] = atomically[R, E, A](new jrocks.WriteOptions())(zio)

  /**
   * Executes the provided zio program in a single transaction.
   */
  def atomically[E >: Throwable, A](writeOptions: jrocks.WriteOptions)(zio: ZIO[Transaction, E, A])(
    implicit A: Atomically.TransactionOnly
  ): ZIO[Any, E, A]

  /**
   * Executes the provided zio program in a single transaction.
   */
  def atomically[E >: Throwable, A](zio: ZIO[Transaction, E, A])(
    implicit A: Atomically.TransactionOnly
  ): ZIO[Any, E, A] = atomically[E, A](new jrocks.WriteOptions())(zio)
}
