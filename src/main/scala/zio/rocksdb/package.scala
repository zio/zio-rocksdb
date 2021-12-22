package zio

package object rocksdb {
  type Bytes         = Chunk[Byte]
  type RocksDB       = service.RocksDB
  type TransactionDB = service.TransactionDB
  type Transaction   = service.Transaction
}
