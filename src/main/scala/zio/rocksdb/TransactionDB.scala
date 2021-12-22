package zio.rocksdb

import org.{ rocksdb => jrocks }
import zio._

object TransactionDB extends Operations[TransactionDB, service.TransactionDB] {
  private final class Live private (db: jrocks.TransactionDB) extends RocksDB.Live(db, Nil) with service.TransactionDB {
    override def beginTransaction(writeOptions: jrocks.WriteOptions): ZManaged[Any, Throwable, service.Transaction] =
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

    private def closeE: Task[Unit] = Task { db.closeE() }
  }

  object Live {
    def open(
      options: jrocks.Options,
      transactionDBOptions: jrocks.TransactionDBOptions,
      path: String
    ): Managed[Throwable, service.TransactionDB] =
      Task(new Live(jrocks.TransactionDB.open(options, transactionDBOptions, path)))
        .toManagedWith(_.closeE.orDie)

    def open(options: jrocks.Options, path: String): Managed[Throwable, service.TransactionDB] =
      open(options, new jrocks.TransactionDBOptions(), path)
    def open(path: String): Managed[Throwable, service.TransactionDB] =
      open(new jrocks.Options(), path)
  }

  def live(
    options: jrocks.Options,
    transactionDBOptions: jrocks.TransactionDBOptions,
    path: String
  ): ZLayer[Any, Throwable, TransactionDB] = Live.open(options, transactionDBOptions, path).toLayer

  def live(options: jrocks.Options, path: String): ZLayer[Any, Throwable, TransactionDB] =
    live(options, new jrocks.TransactionDBOptions(), path)

  def beginTransaction(writeOptions: jrocks.WriteOptions): ZManaged[TransactionDB, Throwable, service.Transaction] =
    for {
      db          <- ZManaged.service[TransactionDB]
      transaction <- db.beginTransaction(writeOptions)
    } yield transaction

  def beginTransaction(): ZManaged[TransactionDB, Throwable, service.Transaction] =
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
