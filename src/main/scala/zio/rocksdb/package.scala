package zio

package object rocksdb {
  type Bytes         = Chunk[Byte]
  type RocksDB       = Has[service.RocksDB]
  type TransactionDB = Has[service.TransactionDB]
  type Transaction   = Has[service.Transaction]
}
