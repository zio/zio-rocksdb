package zio.rocksdb.transaction
import org.rocksdb.WriteOptions
import org.{ rocksdb => jrocks }
import zio._

final class ZTransaction private (jTransaction: jrocks.Transaction) extends RocksDB.TransactionService {
  override def get(readOptions: jrocks.ReadOptions, key: Bytes): Task[Option[Bytes]] = Task {
    Option(jTransaction.get(readOptions, key))
  }
  def getForUpdate(readOptions: jrocks.ReadOptions, key: Bytes, exclusive: Boolean): Task[Option[Bytes]] = Task {
    Option(jTransaction.getForUpdate(readOptions, key, exclusive))
  }
  def put(key: Bytes, value: Bytes): Task[Unit] = Task {
    jTransaction.put(key, value)
  }
  def delete(key: Bytes): Task[Unit] = Task {
    jTransaction.delete(key)
  }
  def commit: Task[Unit] = Task {
    jTransaction.commit()
  }
  def close: UIO[Unit] = UIO {
    jTransaction.close()
  }
  override def rollback: Task[Unit] = Task {
    jTransaction.rollback()
  }
}
object ZTransaction {
  def apply(jTransaction: UIO[jrocks.Transaction]): ZIO[Any, Nothing, ZTransaction] =
    jTransaction.map(new ZTransaction(_))

  def live: ZLayer[TransactionDB, Throwable, Transaction] = live(new jrocks.WriteOptions())
  def live(writeOptions: jrocks.WriteOptions): ZLayer[TransactionDB, Throwable, Transaction] =
    ZLayer.fromManaged(make(writeOptions))

  private def make(writeOptions: WriteOptions): ZManaged[TransactionDB, Nothing, RocksDB.TransactionService] =
    (for {
      managedTransaction <- ZIO.accessM[TransactionDB](_.get.beginTransaction(writeOptions))
    } yield managedTransaction).toManaged(k => k.commit.orDie)
}
