package zio.rocksdb
import org.rocksdb.ReadOptions
import org.{ rocksdb => jrocks }
import zio._

object Transaction {

  private final class Live private (transaction: jrocks.Transaction) extends service.Transaction {
    override def get(readOptions: jrocks.ReadOptions, key: Array[Byte]): Task[Option[Array[Byte]]] = Task {
      Option(transaction.get(readOptions, key))
    }
    override def getForUpdate(
      readOptions: ReadOptions,
      key: Array[Byte],
      exclusive: Boolean
    ): Task[Option[Array[Byte]]]                                       = Task { Option(transaction.getForUpdate(readOptions, key, exclusive)) }
    override def put(key: Array[Byte], value: Array[Byte]): Task[Unit] = Task { transaction.put(key, value) }
    override def delete(key: Array[Byte]): Task[Unit]                  = Task { transaction.delete(key) }
    override def commit: Task[Unit]                                    = Task { transaction.commit() }
    override def close: UIO[Unit]                                      = UIO { transaction.close() }
    override def rollback: Task[Unit]                                  = Task { transaction.rollback() }
  }

  object Live {
    def begin(
      db: jrocks.TransactionDB,
      writeOptions: jrocks.WriteOptions
    ): ZManaged[Any, Throwable, service.Transaction] =
      UIO(new Live(db.beginTransaction(writeOptions))).toManaged(_.close)
  }

  def live(db: jrocks.TransactionDB, writeOptions: jrocks.WriteOptions): ZLayer[Any, Throwable, Transaction] =
    Live.begin(db, writeOptions).toLayer

  def live(db: jrocks.TransactionDB): ZLayer[Any, Throwable, Transaction] =
    live(db)

  def get(readOptions: jrocks.ReadOptions, key: Array[Byte]): RIO[Transaction, Option[Array[Byte]]] =
    RIO.accessM(_.get.get(readOptions, key))
  def get(key: Array[Byte]): RIO[Transaction, Option[Array[Byte]]] =
    RIO.accessM(_.get.get(key))
  def getForUpdate(
    readOptions: jrocks.ReadOptions,
    key: Array[Byte],
    exclusive: Boolean
  ): RIO[Transaction, Option[Array[Byte]]] =
    RIO.accessM(_.get.getForUpdate(readOptions, key, exclusive))
  def getForUpdate(key: Array[Byte], exclusive: Boolean): RIO[Transaction, Option[Array[Byte]]] =
    RIO.accessM(_.get.getForUpdate(key, exclusive))
  def put(key: Array[Byte], value: Array[Byte]): RIO[Transaction, Unit] =
    RIO.accessM(_.get.put(key, value))
  def delete(key: Array[Byte]): RIO[Transaction, Unit] =
    RIO.accessM(_.get.delete(key))
  def commit: RIO[Transaction, Unit] =
    RIO.accessM(_.get.commit)
  def close: URIO[Transaction, Unit] =
    RIO.accessM(_.get.close)
  def rollback: RIO[Transaction, Unit] =
    RIO.accessM(_.get.rollback)

}
