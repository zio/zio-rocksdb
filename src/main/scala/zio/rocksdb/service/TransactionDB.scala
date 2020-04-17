package zio.rocksdb.service

import org.{ rocksdb => jrocks }
import zio.UIO

trait TransactionDB extends RocksDB {
  def beginTransaction(writeOptions: jrocks.WriteOptions): UIO[Transaction]
  def beginTransaction: UIO[Transaction]
}
