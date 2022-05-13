package zio.rocksdb.service

import org.{ rocksdb => jrocks }
import zio.Task

trait WriteBatch {
  def put(key: Array[Byte], value: Array[Byte]): Task[Unit]

  def put(cfHandle: jrocks.ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Task[Unit]
}
