package zio.rocksdb

import org.{ rocksdb => jrocks }
import zio._

trait TransactionDB extends RocksDB {

  /**
   * Creates a managed instance of `service.Transaction` using the provided `WriteOptions`.
   */
  def beginTransaction(writeOptions: jrocks.WriteOptions): ZIO[Scope, Throwable, Transaction]

  /**
   * Creates a managed instance of `service.Transaction`.
   */
  def beginTransaction: ZIO[Scope, Throwable, Transaction] = beginTransaction(new jrocks.WriteOptions())

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

object TransactionDB extends Operations[TransactionDB] {
  private final class Live private (db: jrocks.TransactionDB) extends RocksDB.Live(db, Nil) with TransactionDB {
    override def beginTransaction(writeOptions: jrocks.WriteOptions): ZIO[Scope, Throwable, Transaction] =
      Transaction.Live.begin(db, writeOptions)

    override def atomically[R, E >: Throwable, A](writeOptions: jrocks.WriteOptions)(
      zio: ZIO[Transaction with R, E, A]
    )(
      implicit A: Atomically.TransactionWithSomething
    ): ZIO[R, E, A] =
      (zio <* Transaction.commit).provideSomeLayer[R](Transaction.live(db, writeOptions))

    override def atomically[E >: Throwable, A](writeOptions: jrocks.WriteOptions)(zio: ZIO[Transaction, E, A])(
      implicit A: Atomically.TransactionOnly
    ): IO[E, A] =
      (zio <* Transaction.commit).provideLayer(Transaction.live(db, writeOptions))

    private def closeE: Task[Unit] = ZIO.attempt { db.closeE() }
  }

  object Live {
    def open(
      options: jrocks.Options,
      transactionDBOptions: jrocks.TransactionDBOptions,
      path: String
    ): ZIO[Scope, Throwable, TransactionDB] =
      ZIO.acquireRelease(ZIO.attempt(new Live(jrocks.TransactionDB.open(options, transactionDBOptions, path))))(
        _.closeE.orDie
      )

    def open(options: jrocks.Options, path: String): ZIO[Scope, Throwable, TransactionDB] =
      open(options, new jrocks.TransactionDBOptions(), path)
    def open(path: String): ZIO[Scope, Throwable, TransactionDB] =
      open(new jrocks.Options(), path)
  }

  def live(
    options: jrocks.Options,
    transactionDBOptions: jrocks.TransactionDBOptions,
    path: String
  ): ZLayer[Any, Throwable, TransactionDB] =
    ZLayer.scoped {
      Live.open(options, transactionDBOptions, path)
    }

  def live(options: jrocks.Options, path: String): ZLayer[Any, Throwable, TransactionDB] =
    live(options, new jrocks.TransactionDBOptions(), path)

  def beginTransaction(writeOptions: jrocks.WriteOptions): ZIO[TransactionDB with Scope, Throwable, Transaction] =
    for {
      db          <- ZIO.service[TransactionDB]
      transaction <- db.beginTransaction(writeOptions)
    } yield transaction

  def beginTransaction(): ZIO[TransactionDB with Scope, Throwable, Transaction] =
    beginTransaction(new jrocks.WriteOptions())

  def atomically[R, E >: Throwable, A](writeOptions: jrocks.WriteOptions)(zio: ZIO[Transaction with R, E, A])(
    implicit A: Atomically.TransactionWithSomething
  ): ZIO[TransactionDB with R, E, A] =
    RIO.service[TransactionDB].flatMap(_.atomically[R, E, A](writeOptions)(zio))

  def atomically[R, E >: Throwable, A](zio: ZIO[Transaction with R, E, A])(
    implicit A: Atomically.TransactionWithSomething
  ): ZIO[TransactionDB with R, E, A] =
    RIO.service[TransactionDB].flatMap(_.atomically[R, E, A](zio))

  def atomically[E >: Throwable, A](writeOptions: jrocks.WriteOptions)(
    zio: ZIO[Transaction, E, A]
  )(implicit A: Atomically.TransactionOnly): ZIO[TransactionDB, E, A] =
    RIO.service[TransactionDB].flatMap(_.atomically[E, A](writeOptions)(zio))

  def atomically[E >: Throwable, A](
    zio: ZIO[Transaction, E, A]
  )(implicit A: Atomically.TransactionOnly): ZIO[TransactionDB, E, A] =
    RIO.service[TransactionDB].flatMap(_.atomically[E, A](zio))
}
