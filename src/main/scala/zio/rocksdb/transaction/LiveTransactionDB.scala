package zio.rocksdb.transaction
import org.rocksdb.WriteOptions
import org.{ rocksdb => jrocks }
import zio.{ UIO, ZLayer, ZManaged }

/**
 * LiveTransactionDB provides a ZIO based api on top of the rocksdb.TransactionDB type.
 */
final class LiveTransactionDB private (transactionDB: jrocks.TransactionDB) extends TransactionDB.Service {
  private def close = UIO {
    transactionDB.close()
  }

  override def beginTransaction(writeOptions: WriteOptions): UIO[TransactionDB.TransactionService] =
    LiveTransaction(UIO(transactionDB.beginTransaction(writeOptions)))
}

object LiveTransactionDB {
  def open(
    options: jrocks.Options,
    path: String
  ): ZManaged[Any, Throwable, TransactionDB.Service] = open(options, new jrocks.TransactionDBOptions(), path)

  def open(
    options: jrocks.Options,
    transactionDBOptions: jrocks.TransactionDBOptions,
    path: String
  ): ZManaged[Any, Throwable, TransactionDB.Service] =
    UIO(new LiveTransactionDB(jrocks.TransactionDB.open(options, transactionDBOptions, path)))
      .toManaged(transactionDB => transactionDB.close)

  def live(
    options: jrocks.Options,
    transactionDBOptions: jrocks.TransactionDBOptions,
    path: String
  ): ZLayer.NoDeps[Throwable, TransactionDB] =
    ZLayer.fromManaged(LiveTransactionDB.open(options, transactionDBOptions, path))

  def live(path: String): ZLayer.NoDeps[Throwable, TransactionDB] =
    live(new jrocks.Options(), new jrocks.TransactionDBOptions(), path)
}
