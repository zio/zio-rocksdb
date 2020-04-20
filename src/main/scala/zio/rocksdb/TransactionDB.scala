package zio.rocksdb

import org.{ rocksdb => jrocks }
import zio._

object TransactionDB extends Operations[TransactionDB, service.TransactionDB] { self =>
  private final class Live(db: jrocks.TransactionDB) extends RocksDB.Live(db, Nil) with service.TransactionDB {
    override def beginTransaction(writeOptions: jrocks.WriteOptions): UIO[service.Transaction] =
      UIO(Transaction(db.beginTransaction(writeOptions)))

    override def atomically[R <: Has[_], E >: Throwable, A](zio: ZIO[Transaction with R, E, A])(
      implicit A: Atomically.TransactionWithSomething
    ): ZIO[R, E, A] =
      (zio <* Transaction.commit).provideSomeLayer[R](super.beginTransaction.toLayer)

    override def atomically[E >: Throwable, A](zio: ZIO[Transaction, E, A])(
      implicit A: Atomically.TransactionOnly
    ): IO[E, A] =
      (zio <* Transaction.commit).provideLayer(super.beginTransaction.toLayer)
  }

  object Live {
    def open(
      options: jrocks.Options,
      transactionDBOptions: jrocks.TransactionDBOptions,
      path: String
    ): Managed[Throwable, service.TransactionDB] =
      Task(jrocks.TransactionDB.open(options, transactionDBOptions, path))
        .toManaged(k => Task(k.closeE()).orDie)
        .map(new Live(_))

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

  def beginTransaction(writeOptions: jrocks.WriteOptions): URIO[TransactionDB, service.Transaction] =
    RIO.accessM[TransactionDB](_.get.beginTransaction(writeOptions))

  def beginTransaction(): URIO[TransactionDB, service.Transaction] =
    beginTransaction(new jrocks.WriteOptions())

  def atomically[R <: Has[_], E >: Throwable, A](zio: ZIO[Transaction with R, E, A])(
    implicit A: Atomically.TransactionWithSomething
  ): ZIO[TransactionDB with R, E, A] =
    RIO.access[TransactionDB](_.get) >>= { _.atomically[R, E, A](zio) }

  def atomically[E >: Throwable, A](
    zio: ZIO[Transaction, E, A]
  )(implicit A: Atomically.TransactionOnly): ZIO[TransactionDB, E, A] =
    RIO.access[TransactionDB](_.get) >>= { _.atomically[E, A](zio) }
}
