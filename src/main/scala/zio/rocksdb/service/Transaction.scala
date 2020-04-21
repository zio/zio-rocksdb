package zio.rocksdb.service

import org.{ rocksdb => jrocks }
import zio.{ Task, UIO }

trait Transaction {

  /**
   * Retrieve a key using this transaction.
   */
  def get(readOptions: jrocks.ReadOptions, key: Array[Byte]): Task[Option[Array[Byte]]]

  /**
   * Retrieve a key using this transaction.
   */
  def get(key: Array[Byte]): Task[Option[Array[Byte]]] = get(new jrocks.ReadOptions(), key)

  /**
   * Retrieve a key that will be updated using this transaction.
   */
  def getForUpdate(readOptions: jrocks.ReadOptions, key: Array[Byte], exclusive: Boolean): Task[Option[Array[Byte]]]

  /**
   * Retrieve a key that will be updated using this transaction.
   */
  def getForUpdate(key: Array[Byte], exclusive: Boolean): Task[Option[Array[Byte]]] =
    getForUpdate(new jrocks.ReadOptions(), key, exclusive)

  /**
   * Writes a key using this transaction.
   */
  def put(key: Array[Byte], value: Array[Byte]): Task[Unit]

  /**
   * Deletes a key using this transaction.
   */
  def delete(key: Array[Byte]): Task[Unit]

  /**
   * Commits all the changes using this transaction.
   */
  def commit: Task[Unit]

  /**
   * Closes the transaction.
   */
  def close: UIO[Unit]

  /**
   * Rollbacks all the changes made through this transaction.
   */
  def rollback: Task[Unit]
}
