package zio.rocksdb

import org.rocksdb.{ ColumnFamilyHandle, ReadOptions, WriteOptions }
import org.{ rocksdb => jrocks }
import zio._

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

  /*
   * Writes a key using this transaction.
   */
  def put(cf: ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Task[Unit]

  /**
   * Deletes a key using this transaction.
   */
  def delete(key: Array[Byte]): Task[Unit]

  /**
   * Commits all the changes using this transaction.
   */
  def commit: Task[Unit]

  /**
   * Closes the transaction.
   */
  def close: UIO[Unit]

  /**
   * Rollbacks all the changes made through this transaction.
   */
  def rollback: Task[Unit]
}

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
        ZIO.debug("acquired permit") *> ZIO.succeed(transaction.put(key, value)) <* ZIO.debug("releasing permit")
      }

    override def delete(key: Array[Byte]): Task[Unit] = taskWithPermit {
      transaction.delete(key)
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

  object Live {
    def begin(
      db: jrocks.TransactionDB,
      writeOptions: jrocks.WriteOptions
    ): ZIO[Scope, Throwable, Transaction] =
      for {
        semaphore   <- Semaphore.make(1)
        transaction <- ZIO.acquireRelease(ZIO.attempt(new Live(semaphore, db.beginTransaction(writeOptions))))(_.close)
      } yield transaction
  }

  def live(db: jrocks.TransactionDB, writeOptions: jrocks.WriteOptions): ZLayer[Any, Throwable, Transaction] =
    ZLayer.scoped {
      Live.begin(db, writeOptions)
    }

  def live(db: jrocks.TransactionDB): ZLayer[Any, Throwable, Transaction] =
    live(db, new WriteOptions())

  def get(readOptions: jrocks.ReadOptions, key: Array[Byte]): RIO[Transaction, Option[Array[Byte]]] =
    RIO.serviceWithZIO(_.get(readOptions, key))

  def get(key: Array[Byte]): RIO[Transaction, Option[Array[Byte]]] =
    RIO.serviceWithZIO(_.get(key))

  def getForUpdate(
    readOptions: jrocks.ReadOptions,
    key: Array[Byte],
    exclusive: Boolean
  ): RIO[Transaction, Option[Array[Byte]]] =
    RIO.serviceWithZIO(_.getForUpdate(readOptions, key, exclusive))

  def getForUpdate(
    readOptions: jrocks.ReadOptions,
    cf: ColumnFamilyHandle,
    key: Array[Byte],
    exclusive: Boolean
  ): RIO[Transaction, Option[Array[Byte]]] =
    RIO.serviceWithZIO(_.getForUpdate(readOptions, cf, key, exclusive))

  def getForUpdate(key: Array[Byte], exclusive: Boolean): RIO[Transaction, Option[Array[Byte]]] =
    RIO.serviceWithZIO(_.getForUpdate(key, exclusive))

  def getForUpdate(
    cf: ColumnFamilyHandle,
    key: Array[Byte],
    exclusive: Boolean
  ): RIO[Transaction, Option[Array[Byte]]] =
    RIO.serviceWithZIO(_.getForUpdate(cf, key, exclusive))

  def put(key: Array[Byte], value: Array[Byte]): RIO[Transaction, Unit] =
    RIO.serviceWithZIO(_.put(key, value))

  def put(cf: ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): RIO[Transaction, Unit] =
    RIO.serviceWithZIO(_.put(cf, key, value))

  def delete(key: Array[Byte]): RIO[Transaction, Unit] =
    RIO.serviceWithZIO(_.delete(key))

  def commit: RIO[Transaction, Unit] =
    RIO.serviceWithZIO(_.commit)

  def close: URIO[Transaction, Unit] =
    RIO.serviceWithZIO(_.close)

  def rollback: RIO[Transaction, Unit] =
    RIO.serviceWithZIO(_.rollback)

}
