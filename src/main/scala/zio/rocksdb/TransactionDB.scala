package zio.rocksdb

import org.{ rocksdb => jrocks }
import zio._

object TransactionDB extends Operations[TransactionDB, service.TransactionDB] { self =>
  private final class Live(db: jrocks.TransactionDB) extends RocksDB.Live(db, Nil) with service.TransactionDB {
    override def beginTransaction(writeOptions: jrocks.WriteOptions): ZManaged[Any, Nothing, service.Transaction] =
      Transaction.begin(db, writeOptions)

    override def atomically[R <: Has[_], E >: Throwable, A](writeOptions: jrocks.WriteOptions)(
      zio: ZIO[Transaction with R, E, A]
    )(
      implicit A: Atomically.TransactionWithSomething
    ): ZIO[R, E, A] =
      (zio <* Transaction.commit).provideSomeLayer[R](Transaction.live(db, writeOptions))

    override def atomically[E >: Throwable, A](writeOptions: jrocks.WriteOptions)(zio: ZIO[Transaction, E, A])(
      implicit A: Atomically.TransactionOnly
    ): IO[E, A] =
      (zio <* Transaction.commit).provideLayer(Transaction.live(db, writeOptions))
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

  def beginTransaction(writeOptions: jrocks.WriteOptions): ZManaged[TransactionDB, Nothing, service.Transaction] =
    for {
      db          <- ZIO.access[TransactionDB](_.get).toManaged_
      transaction <- db.beginTransaction(writeOptions)
    } yield transaction

  def beginTransaction(): ZManaged[TransactionDB, Nothing, service.Transaction] =
    beginTransaction(new jrocks.WriteOptions())

  def atomically[R <: Has[_], E >: Throwable, A](writeOptions: jrocks.WriteOptions)(zio: ZIO[Transaction with R, E, A])(
    implicit A: Atomically.TransactionWithSomething
  ): ZIO[TransactionDB with R, E, A] =
    RIO.access[TransactionDB](_.get) >>= { _.atomically[R, E, A](writeOptions)(zio) }

  def atomically[R <: Has[_], E >: Throwable, A](zio: ZIO[Transaction with R, E, A])(
    implicit A: Atomically.TransactionWithSomething
  ): ZIO[TransactionDB with R, E, A] =
    RIO.access[TransactionDB](_.get) >>= { _.atomically[R, E, A](zio) }

  def atomically[E >: Throwable, A](writeOptions: jrocks.WriteOptions)(
    zio: ZIO[Transaction, E, A]
  )(implicit A: Atomically.TransactionOnly): ZIO[TransactionDB, E, A] =
    RIO.access[TransactionDB](_.get) >>= { _.atomically[E, A](writeOptions)(zio) }

  def atomically[E >: Throwable, A](
    zio: ZIO[Transaction, E, A]
  )(implicit A: Atomically.TransactionOnly): ZIO[TransactionDB, E, A] =
    RIO.access[TransactionDB](_.get) >>= { _.atomically[E, A](zio) }
}
