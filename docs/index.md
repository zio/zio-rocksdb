---
id: index
title: "Introduction to ZIO RocksDB"
sidebar_label: "ZIO RocksDB"
---

[ZIO RocksDB](https://github.com/zio/zio-rocksdb) is a ZIO-based interface to RocksDB.

Rocksdb is an embeddable persistent key-value store that is optimized for fast storage. ZIO RocksDB provides us a functional ZIO wrapper around its Java API.

@PROJECT_BADGES@

## Installation

In order to use this library, we need to add the following line in our `build.sbt` file:

```scala
libraryDependencies += "dev.zio" %% "zio-rocksdb" % "@VERSION@"
```

## Example 1

Use the provided `RocksDB` wrapper:

```scala mdoc:compile-only
import java.nio.charset.StandardCharsets.UTF_8
import zio.rocksdb.RocksDB

val key   = "key".getBytes(UTF_8)
val value = "value".getBytes(UTF_8)

val database  = RocksDB.live("/data/state")
val readWrite = RocksDB.put(key, value) *> RocksDB.get(key)
val result    = readWrite.provide(database)
```

## Example 2

An example of writing and reading key/value pairs and also using transactional operations when using RocksDB:

```scala mdoc:compile-only
import zio._
import zio.rocksdb._

import java.nio.charset.StandardCharsets._

object ZIORocksDBExample extends ZIOAppDefault {

  private def bytesToString(bytes: Array[Byte]): String = new String(bytes, UTF_8)
  private def bytesToInt(bytes: Array[Byte]): Int       = bytesToString(bytes).toInt

  val job1: ZIO[RocksDB, Throwable, Unit] =
    for {
      _ <- RocksDB.put(
        "Key".getBytes(UTF_8),
        "Value".getBytes(UTF_8)
      )
      result       <- RocksDB.get("Key".getBytes(UTF_8))
      stringResult = result.map(bytesToString)
      _            <- Console.printLine(s"value: $stringResult")
    } yield ()

  val job2: ZIO[TransactionDB, Throwable, Unit] =
    for {
      key <- ZIO.succeed("COUNT".getBytes(UTF_8))
      _   <- TransactionDB.put(key, 0.toString.getBytes(UTF_8))
      _ <- ZIO.foreachParDiscard(0 until 10) { _ =>
        TransactionDB.atomically {
          Transaction.getForUpdate(key, exclusive = true) flatMap { iCount =>
            Transaction.put(key, iCount.map(bytesToInt).map(_ + 1).getOrElse(-1).toString.getBytes(UTF_8))
          }
        }
      }
      value        <- TransactionDB.get(key)
      counterValue = value.map(bytesToInt)
      _            <- Console.printLine(s"The value of counter: $counterValue") // Must be 10
    } yield ()

  private val transactional_db =
    TransactionDB.live(new org.rocksdb.Options().setCreateIfMissing(true), "tr_db")

  private val rocks_db =
    RocksDB.live(new org.rocksdb.Options().setCreateIfMissing(true), "rocks_db")

  def run =
    (job1.provide(rocks_db) <*> job2.provide(transactional_db))
      .foldCauseZIO(cause => Console.printLine(cause.prettyPrint) *> ZIO.succeed(1), _ => ZIO.succeed(0))
}
```
