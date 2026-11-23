package zio.rocksdb

import org.{ rocksdb => jrocks }
import zio._
import zio.rocksdb.RocksDB.Live.rawDefaultColumnFamily

import scala.jdk.CollectionConverters._

/**
 * A RocksDB database opened with transaction support. It extends `RocksDB`, so every method of `RocksDB` also
 * works on a `TransactionDB`. Open one with `TransactionDB.live`, which closes the database when its scope
 * closes.
 *
 * {{{
 * import zio._
 * import zio.rocksdb._
 * import org.rocksdb.Options
 *
 * // A layer that opens the database at the path and closes it with its scope.
 * val db: ZLayer[Any, Throwable, TransactionDB] =
 *   TransactionDB.live(new Options().setCreateIfMissing(true), "/tmp/zio-rocksdb-example")
 * }}}
 *
 * @see [[https://zio.dev/zio-rocksdb/ ZIO RocksDB]]
 */
trait TransactionDB extends RocksDB {

  /**
   * Creates a managed instance of `Transaction` using the provided `WriteOptions`.
   */
  def beginTransaction(writeOptions: jrocks.WriteOptions): ZIO[Scope, Throwable, Transaction]

  /**
   * Creates a managed instance of `Transaction`.
   */
  def beginTransaction: ZIO[Scope, Throwable, Transaction] = beginTransaction(new jrocks.WriteOptions())

  /**
   * Runs `zio` in a transaction and commits it when `zio` succeeds. When `zio` fails, the transaction is
   * closed without a commit, so its writes are discarded.
   */
  def atomically[R, E >: Throwable, A](writeOptions: jrocks.WriteOptions)(
    zio: ZIO[Transaction with R, E, A]
  )(implicit A: Atomically.TransactionWithSomething): ZIO[R, E, A]

  /**
   * Runs `zio` in a transaction and commits it when `zio` succeeds. When `zio` fails, the transaction is
   * closed without a commit, so its writes are discarded.
   */
  def atomically[R, E >: Throwable, A](zio: ZIO[Transaction with R, E, A])(
    implicit A: Atomically.TransactionWithSomething
  ): ZIO[R, E, A] = atomically[R, E, A](new jrocks.WriteOptions())(zio)

  /**
   * Runs `zio` in a transaction and commits it when `zio` succeeds. When `zio` fails, the transaction is
   * closed without a commit, so its writes are discarded.
   */
  def atomically[E >: Throwable, A](writeOptions: jrocks.WriteOptions)(zio: ZIO[Transaction, E, A])(
    implicit A: Atomically.TransactionOnly
  ): ZIO[Any, E, A]

  /**
   * Runs `zio` in a transaction and commits it when `zio` succeeds. When `zio` fails, the transaction is
   * closed without a commit, so its writes are discarded.
   *
   * {{{
   * import zio._
   * import zio.rocksdb._
   * import java.nio.charset.StandardCharsets.UTF_8
   *
   * val key: Array[Byte] = "balance".getBytes(UTF_8)
   *
   * // Reads the balance under an exclusive lock, adds 10 and writes it back in one transaction.
   * val credit: RIO[TransactionDB, Unit] =
   *   TransactionDB.atomically {
   *     for {
   *       current <- Transaction.getForUpdate(key, exclusive = true)
   *       next     = current.fold(0L)(bytes => new String(bytes, UTF_8).toLong) + 10L
   *       _       <- Transaction.put(key, next.toString.getBytes(UTF_8))
   *     } yield ()
   *   }
   * }}}
   */
  def atomically[E >: Throwable, A](zio: ZIO[Transaction, E, A])(
    implicit A: Atomically.TransactionOnly
  ): ZIO[Any, E, A] = atomically[E, A](new jrocks.WriteOptions())(zio)
}

/**
 * Constructors and accessors for `TransactionDB`. The accessors read the `TransactionDB` from the ZIO
 * environment.
 */
object TransactionDB extends Operations[TransactionDB] {
  private final class Live private (db: jrocks.TransactionDB, cfHandles: List[jrocks.ColumnFamilyHandle])
      extends RocksDB.Live(db, cfHandles)
      with TransactionDB {
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

  /**
   * Opens a `TransactionDB` as a scoped resource. The database is closed when the scope closes.
   */
  object Live {

    /**
     * Opens the database at `path` with the ColumnFamilies in `columnFamilyDescriptors`. The handles of those
     * ColumnFamilies are returned by `initialHandles`.
     */
    def open(
      options: jrocks.DBOptions,
      transactionDBOptions: jrocks.TransactionDBOptions,
      path: String,
      columnFamilyDescriptors: List[jrocks.ColumnFamilyDescriptor]
    ): ZIO[Scope, Throwable, TransactionDB] = {
      val handles = new java.util.ArrayList[jrocks.ColumnFamilyHandle](columnFamilyDescriptors.size)
      ZIO
        .acquireRelease(
          ZIO.attempt(
            jrocks.TransactionDB.open(options, transactionDBOptions, path, columnFamilyDescriptors.asJava, handles)
          )
        )(db => ZIO.attempt(db.closeE()).orDie)
        .map(db => new Live(db, handles.asScala.toList))
    }

    /**
     * Opens the database at `path` with only the default ColumnFamily.
     */
    def open(
      options: jrocks.Options,
      transactionDBOptions: jrocks.TransactionDBOptions,
      path: String
    ): ZIO[Scope, Throwable, TransactionDB] =
      ZIO.acquireRelease(ZIO.attempt(new Live(jrocks.TransactionDB.open(options, transactionDBOptions, path), Nil)))(
        _.closeE.orDie
      )

    /**
     * Opens the database at `path` with `options` and default `TransactionDBOptions`.
     */
    def open(options: jrocks.Options, path: String): ZIO[Scope, Throwable, TransactionDB] =
      open(options, new jrocks.TransactionDBOptions(), path)

    /**
     * Opens the database at `path` with default options and default `TransactionDBOptions`.
     */
    def open(path: String): ZIO[Scope, Throwable, TransactionDB] =
      open(new jrocks.Options(), path)

    /**
     * Opens the database at `path` with every ColumnFamily that the database directory lists. Each ColumnFamily
     * uses `columnFamilyOptions`.
     */
    def openAllColumnFamilies(
      options: jrocks.DBOptions,
      columnFamilyOptions: jrocks.ColumnFamilyOptions,
      transactionDBOptions: jrocks.TransactionDBOptions,
      path: String
    ): ZIO[Scope, Throwable, TransactionDB] =
      for {
        rawColumnFamilies <- RocksDB.Live.listColumnFamilies(new jrocks.Options(options, columnFamilyOptions), path)
        rawColumnFamiliesWithDefault = if (rawColumnFamilies.exists(_.sameElements(rawColumnFamilies)))
          rawColumnFamilies
        else
          rawDefaultColumnFamily :: rawColumnFamilies
        columnFamilies = rawColumnFamiliesWithDefault.map(bytes => new jrocks.ColumnFamilyDescriptor(bytes))
        live           <- open(options, transactionDBOptions, path, columnFamilies)
      } yield live
  }

  /**
   * A layer that opens the database at `path` with `transactionDBOptions`, and closes it when the layer's
   * scope closes.
   */
  def live(
    options: jrocks.Options,
    transactionDBOptions: jrocks.TransactionDBOptions,
    path: String
  ): ZLayer[Any, Throwable, TransactionDB] =
    ZLayer.scoped {
      Live.open(options, transactionDBOptions, path)
    }

  /**
   * A layer that opens the database at `path` with default `TransactionDBOptions`, and closes it when the
   * layer's scope closes.
   */
  def live(options: jrocks.Options, path: String): ZLayer[Any, Throwable, TransactionDB] =
    live(options, new jrocks.TransactionDBOptions(), path)

  /**
   * A layer that opens every ColumnFamily that the database at `path` lists, and closes the database when the
   * layer's scope closes.
   */
  def liveAllColumnFamilies(
    options: jrocks.DBOptions,
    columnFamilyOptions: jrocks.ColumnFamilyOptions,
    transactionDBOptions: jrocks.TransactionDBOptions,
    path: String
  ): ZLayer[Any, Throwable, TransactionDB] =
    ZLayer.scoped(Live.openAllColumnFamilies(options, columnFamilyOptions, transactionDBOptions, path))

  /**
   * Begins a transaction on the `TransactionDB` in the environment, with `writeOptions`. The transaction is
   * closed when the enclosing `Scope` closes.
   */
  def beginTransaction(writeOptions: jrocks.WriteOptions): ZIO[TransactionDB with Scope, Throwable, Transaction] =
    for {
      db          <- ZIO.service[TransactionDB]
      transaction <- db.beginTransaction(writeOptions)
    } yield transaction

  /**
   * Begins a transaction on the `TransactionDB` in the environment, with default `WriteOptions`. The transaction
   * is closed when the enclosing `Scope` closes.
   */
  def beginTransaction(): ZIO[TransactionDB with Scope, Throwable, Transaction] =
    beginTransaction(new jrocks.WriteOptions())

  /**
   * Runs `zio` in a transaction on the `TransactionDB` in the environment, with `writeOptions`. The transaction
   * commits when `zio` succeeds, and its writes are discarded when `zio` fails.
   */
  def atomically[R, E >: Throwable, A](writeOptions: jrocks.WriteOptions)(zio: ZIO[Transaction with R, E, A])(
    implicit A: Atomically.TransactionWithSomething
  ): ZIO[TransactionDB with R, E, A] =
    ZIO.service[TransactionDB].flatMap(_.atomically[R, E, A](writeOptions)(zio))

  /**
   * Runs `zio` in a transaction on the `TransactionDB` in the environment. The transaction commits when `zio`
   * succeeds, and its writes are discarded when `zio` fails.
   */
  def atomically[R, E >: Throwable, A](zio: ZIO[Transaction with R, E, A])(
    implicit A: Atomically.TransactionWithSomething
  ): ZIO[TransactionDB with R, E, A] =
    ZIO.service[TransactionDB].flatMap(_.atomically[R, E, A](zio))

  /**
   * Runs `zio` in a transaction on the `TransactionDB` in the environment, with `writeOptions`. The transaction
   * commits when `zio` succeeds, and its writes are discarded when `zio` fails.
   */
  def atomically[E >: Throwable, A](writeOptions: jrocks.WriteOptions)(
    zio: ZIO[Transaction, E, A]
  )(implicit A: Atomically.TransactionOnly): ZIO[TransactionDB, E, A] =
    ZIO.service[TransactionDB].flatMap(_.atomically[E, A](writeOptions)(zio))

  /**
   * Runs `zio` in a transaction on the `TransactionDB` in the environment. The transaction commits when `zio`
   * succeeds, and its writes are discarded when `zio` fails.
   */
  def atomically[E >: Throwable, A](
    zio: ZIO[Transaction, E, A]
  )(implicit A: Atomically.TransactionOnly): ZIO[TransactionDB, E, A] =
    ZIO.service[TransactionDB].flatMap(_.atomically[E, A](zio))
}
