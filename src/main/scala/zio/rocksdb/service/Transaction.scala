package zio.rocksdb.service

import org.{ rocksdb => jrocks }
import zio.{ Task, UIO }

trait Transaction {
  def get(readOptions: jrocks.ReadOptions, key: Array[Byte]): Task[Option[Array[Byte]]]
  def get(key: Array[Byte]): Task[Option[Array[Byte]]] = get(new jrocks.ReadOptions(), key)
  def getForUpdate(readOptions: jrocks.ReadOptions, key: Array[Byte], exclusive: Boolean): Task[Option[Array[Byte]]]
  def getForUpdate(key: Array[Byte], exclusive: Boolean): Task[Option[Array[Byte]]] =
    getForUpdate(new jrocks.ReadOptions(), key, exclusive)
  def put(key: Array[Byte], value: Array[Byte]): Task[Unit]
  def delete(key: Array[Byte]): Task[Unit]
  def commit: Task[Unit]
  def close: UIO[Unit]
  def rollback: Task[Unit]
}
