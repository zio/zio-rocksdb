package zio.rocksdb.transaction
import org.rocksdb.WriteOptions
import org.{ rocksdb => jrocks }
import zio.{ UIO, ZLayer, ZManaged }
import zio.rocksdb.RocksDB

/**
 * LiveTransactionDB provides a ZIO based api on top of the rocksdb.TransactionDB type.
 */
final class Live private (transactionDB: jrocks.TransactionDB)
    extends zio.rocksdb.Live(transactionDB, Nil)
    with TransactionDB.Service {
  private def close = UIO {
    transactionDB.close()
  }

  override def beginTransaction(writeOptions: WriteOptions): UIO[TransactionDB.TransactionService] =
    LiveTransaction(UIO(transactionDB.beginTransaction(writeOptions)))

  def asRocksDB: RocksDB.Service = this
}

object Live {
  def open(path: String): ZManaged[Any, Throwable, TransactionDB.Service] =
    open(new jrocks.Options().setCreateIfMissing(true), path)

  def open(
    options: jrocks.Options,
    path: String
  ): ZManaged[Any, Throwable, TransactionDB.Service] = open(options, new jrocks.TransactionDBOptions(), path)

  def open(
    options: jrocks.Options,
    transactionDBOptions: jrocks.TransactionDBOptions,
    path: String
  ): ZManaged[Any, Throwable, TransactionDB.Service] =
    UIO(new Live(jrocks.TransactionDB.open(options, transactionDBOptions, path)))
      .toManaged(transactionDB => transactionDB.close)

  def live(
    options: jrocks.Options,
    transactionDBOptions: jrocks.TransactionDBOptions,
    path: String
  ): ZLayer.NoDeps[Throwable, TransactionDB] =
    ZLayer.fromManaged(Live.open(options, transactionDBOptions, path))

  def live(path: String): ZLayer.NoDeps[Throwable, TransactionDB] =
    live(new jrocks.Options(), new jrocks.TransactionDBOptions(), path)
}
