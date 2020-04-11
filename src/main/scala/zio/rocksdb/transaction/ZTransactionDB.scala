package zio.rocksdb.transaction
import org.rocksdb.WriteOptions
import org.{ rocksdb => jrocks }
import zio.{ UIO, ZLayer, ZManaged }

final class ZTransactionDB private (transactionDB: jrocks.TransactionDB) extends RocksDB.TransactionDBService {
  private def close = UIO {
    transactionDB.close()
  }

  override def beginTransaction(writeOptions: WriteOptions): UIO[RocksDB.TransactionService] =
    ZTransaction(UIO(transactionDB.beginTransaction(writeOptions)))
}

object ZTransactionDB {
  def open(
    options: jrocks.Options,
    path: String
  ): ZManaged[Any, Throwable, RocksDB.TransactionDBService] = open(options, new jrocks.TransactionDBOptions(), path)

  def open(
    options: jrocks.Options,
    transactionDBOptions: jrocks.TransactionDBOptions,
    path: String
  ): ZManaged[Any, Throwable, RocksDB.TransactionDBService] =
    UIO(new ZTransactionDB(jrocks.TransactionDB.open(options, transactionDBOptions, path)))
      .toManaged(transactionDB => transactionDB.close)

  def live(
    options: jrocks.Options,
    transactionDBOptions: jrocks.TransactionDBOptions,
    path: String
  ): ZLayer.NoDeps[Throwable, TransactionDB] =
    ZLayer.fromManaged(ZTransactionDB.open(options, transactionDBOptions, path))

  def live(path: String): ZLayer.NoDeps[Throwable, TransactionDB] =
    live(new jrocks.Options(), new jrocks.TransactionDBOptions(), path)
}
