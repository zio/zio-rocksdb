package zio.rocksdb

import org.rocksdb.{ ColumnFamilyHandle, ReadOptions, WriteOptions }
import org.{ rocksdb => jrocks }
import zio._

/**
 * A RocksDB transaction. Writes made through a transaction become visible to other readers only when it is
 * committed. Operations on one transaction run one at a time. Obtain a transaction with
 * `TransactionDB.beginTransaction` or from the program that `TransactionDB.atomically` runs.
 *
 * {{{
 * import zio._
 * import zio.rocksdb._
 * import java.nio.charset.StandardCharsets.UTF_8
 *
 * // Begins a transaction, writes a key and reads it back before the commit.
 * val manual: ZIO[TransactionDB with Scope, Throwable, Option[Array[Byte]]] =
 *   for {
 *     tx   <- TransactionDB.beginTransaction()
 *     _    <- tx.put("k".getBytes(UTF_8), "v".getBytes(UTF_8))
 *     seen <- tx.get("k".getBytes(UTF_8))
 *     _    <- tx.commit
 *   } yield seen
 * }}}
 */
trait Transaction {

  /**
   * Retrieve a key using this transaction.
   */
  def get(readOptions: jrocks.ReadOptions, key: Array[Byte]): Task[Option[Array[Byte]]]

  /**
   * Retrieve a key using this transaction.
   */
  def get(key: Array[Byte]): Task[Option[Array[Byte]]] = get(new jrocks.ReadOptions(), key)

  /**
   * Retrieve a key that will be updated using this transaction.
   */
  def getForUpdate(readOptions: jrocks.ReadOptions, key: Array[Byte], exclusive: Boolean): Task[Option[Array[Byte]]]

  /**
   * Retrieve a key that will be updated using this transaction.
   */
  def getForUpdate(key: Array[Byte], exclusive: Boolean): Task[Option[Array[Byte]]] =
    getForUpdate(new jrocks.ReadOptions(), key, exclusive)

  /**
   * Retrieve a key that will be updated using this transaction.
   */
  def getForUpdate(
    readOptions: jrocks.ReadOptions,
    cf: ColumnFamilyHandle,
    key: Array[Byte],
    exclusive: Boolean
  ): Task[Option[Array[Byte]]]

  /**
   * Retrieve a key that will be updated using this transaction.
   */
  def getForUpdate(cf: ColumnFamilyHandle, key: Array[Byte], exclusive: Boolean): Task[Option[Array[Byte]]] =
    getForUpdate(new jrocks.ReadOptions(), cf, key, exclusive)

  /**
   * Writes a key using this transaction.
   */
  def put(key: Array[Byte], value: Array[Byte]): Task[Unit]

  /**
   * Writes a key using this transaction.
   */
  def put(cf: ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Task[Unit]

  /**
   * Deletes a key using this transaction.
   */
  def delete(key: Array[Byte]): Task[Unit]

  /**
   * Deletes a key using this transaction from a given column family
   */
  def delete(cfHandle: ColumnFamilyHandle, key: Array[Byte]): Task[Unit]

  /**
   * Commits all the changes using this transaction.
   */
  def commit: Task[Unit]

  /**
   * Closes the transaction.
   */
  def close: UIO[Unit]

  /**
   * Rolls back all the changes made through this transaction.
   */
  def rollback: Task[Unit]
}

/**
 * Constructors and accessors for `Transaction`. The accessors read the `Transaction` from the ZIO environment.
 */
object Transaction {

  private final class Live private (semaphore: Semaphore, transaction: jrocks.Transaction) extends Transaction {

    def taskWithPermit[A](task: => A): Task[A] = semaphore.withPermit(ZIO.attempt(task))

    def uioWithPermit[A](task: => A): UIO[A] = semaphore.withPermit(ZIO.succeed(task))

    override def get(readOptions: jrocks.ReadOptions, key: Array[Byte]): Task[Option[Array[Byte]]] = taskWithPermit {
      Option(transaction.get(readOptions, key))
    }

    override def getForUpdate(
      readOptions: ReadOptions,
      key: Array[Byte],
      exclusive: Boolean
    ): Task[Option[Array[Byte]]] = taskWithPermit {
      Option(transaction.getForUpdate(readOptions, key, exclusive))
    }

    override def put(key: Array[Byte], value: Array[Byte]): Task[Unit] =
      semaphore.withPermit {
        ZIO.attempt(transaction.put(key, value))
      }

    override def delete(key: Array[Byte]): Task[Unit] = taskWithPermit {
      transaction.delete(key)
    }

    override def delete(cfHandle: ColumnFamilyHandle, key: Array[Byte]): Task[Unit] = taskWithPermit {
      transaction.delete(cfHandle, key)
    }

    override def commit: Task[Unit] = taskWithPermit {
      transaction.commit()
    }

    override def close: UIO[Unit] = uioWithPermit {
      transaction.close()
    }

    override def rollback: Task[Unit] = taskWithPermit {
      transaction.rollback()
    }

    override def getForUpdate(
      readOptions: ReadOptions,
      cf: ColumnFamilyHandle,
      key: Array[Byte],
      exclusive: Boolean
    ): Task[Option[Array[Byte]]] = taskWithPermit {
      Option(transaction.getForUpdate(readOptions, cf, key, exclusive))
    }

    override def put(cf: ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Task[Unit] = taskWithPermit {
      transaction.put(cf, key, value)
    }
  }

  /**
   * The `Transaction` implementation for a `jrocks.TransactionDB`.
   */
  object Live {

    /**
     * Begins a transaction on `db` with `writeOptions`. The transaction is closed when the enclosing `Scope`
     * closes.
     */
    def begin(
      db: jrocks.TransactionDB,
      writeOptions: jrocks.WriteOptions
    ): ZIO[Scope, Throwable, Transaction] =
      for {
        semaphore   <- Semaphore.make(1)
        transaction <- ZIO.acquireRelease(ZIO.attempt(new Live(semaphore, db.beginTransaction(writeOptions))))(_.close)
      } yield transaction
  }

  /**
   * A layer that begins a transaction on `db` with `writeOptions` and closes it when the layer's scope
   * closes.
   */
  def live(db: jrocks.TransactionDB, writeOptions: jrocks.WriteOptions): ZLayer[Any, Throwable, Transaction] =
    ZLayer.scoped {
      Live.begin(db, writeOptions)
    }

  /**
   * A layer that begins a transaction on `db` with default `WriteOptions`, and closes it when the layer's scope
   * closes.
   */
  def live(db: jrocks.TransactionDB): ZLayer[Any, Throwable, Transaction] =
    live(db, new WriteOptions())

  /**
   * Reads `key` in the transaction in the environment, with `readOptions`.
   */
  def get(readOptions: jrocks.ReadOptions, key: Array[Byte]): RIO[Transaction, Option[Array[Byte]]] =
    ZIO.serviceWithZIO(_.get(readOptions, key))

  /**
   * Reads `key` in the transaction in the environment, with default `ReadOptions`.
   */
  def get(key: Array[Byte]): RIO[Transaction, Option[Array[Byte]]] =
    ZIO.serviceWithZIO(_.get(key))

  /**
   * Reads `key` in the transaction in the environment, and locks it for the rest of the transaction. The
   * `exclusive` flag selects an exclusive lock when it is `true`, and a shared lock when it is `false`.
   */
  def getForUpdate(
    readOptions: jrocks.ReadOptions,
    key: Array[Byte],
    exclusive: Boolean
  ): RIO[Transaction, Option[Array[Byte]]] =
    ZIO.serviceWithZIO(_.getForUpdate(readOptions, key, exclusive))

  /**
   * Reads `key` from the ColumnFamily `cf` in the transaction in the environment, and locks it for the rest of
   * the transaction. The `exclusive` flag selects an exclusive lock when it is `true`, and a shared lock when
   * it is `false`.
   */
  def getForUpdate(
    readOptions: jrocks.ReadOptions,
    cf: ColumnFamilyHandle,
    key: Array[Byte],
    exclusive: Boolean
  ): RIO[Transaction, Option[Array[Byte]]] =
    ZIO.serviceWithZIO(_.getForUpdate(readOptions, cf, key, exclusive))

  /**
   * Reads `key` in the transaction in the environment with default `ReadOptions`, and locks it for the rest of
   * the transaction. The `exclusive` flag selects an exclusive lock when it is `true`, and a shared lock when
   * it is `false`.
   */
  def getForUpdate(key: Array[Byte], exclusive: Boolean): RIO[Transaction, Option[Array[Byte]]] =
    ZIO.serviceWithZIO(_.getForUpdate(key, exclusive))

  /**
   * Reads `key` from the ColumnFamily `cf` in the transaction in the environment with default `ReadOptions`,
   * and locks it for the rest of the transaction. The `exclusive` flag selects an exclusive lock when it is
   * `true`, and a shared lock when it is `false`.
   */
  def getForUpdate(
    cf: ColumnFamilyHandle,
    key: Array[Byte],
    exclusive: Boolean
  ): RIO[Transaction, Option[Array[Byte]]] =
    ZIO.serviceWithZIO(_.getForUpdate(cf, key, exclusive))

  /**
   * Writes `value` under `key` in the transaction in the environment.
   */
  def put(key: Array[Byte], value: Array[Byte]): RIO[Transaction, Unit] =
    ZIO.serviceWithZIO(_.put(key, value))

  /**
   * Writes `value` under `key` in the ColumnFamily `cf`, as part of the transaction in the environment.
   */
  def put(cf: ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): RIO[Transaction, Unit] =
    ZIO.serviceWithZIO(_.put(cf, key, value))

  /**
   * Deletes `key` as part of the transaction in the environment.
   */
  def delete(key: Array[Byte]): RIO[Transaction, Unit] =
    ZIO.serviceWithZIO(_.delete(key))

  /**
   * Deletes `key` from the ColumnFamily `cf`, as part of the transaction in the environment.
   */
  def delete(cf: ColumnFamilyHandle, key: Array[Byte]): RIO[Transaction, Unit] =
    ZIO.serviceWithZIO(_.delete(cf, key))

  /**
   * Commits the transaction in the environment, which makes its writes visible to other readers.
   */
  def commit: RIO[Transaction, Unit] =
    ZIO.serviceWithZIO(_.commit)

  /**
   * Closes the transaction in the environment. A transaction that was not committed discards its writes.
   */
  def close: URIO[Transaction, Unit] =
    ZIO.serviceWithZIO(_.close)

  /**
   * Rolls back the writes made by the transaction in the environment.
   */
  def rollback: RIO[Transaction, Unit] =
    ZIO.serviceWithZIO(_.rollback)

}
