---
id: index
title: "Introduction to ZIO RocksDB"
sidebar_label: "ZIO RocksDB"
---

A ZIO-based interface to RocksDB.

## Installation

Add the following dependencies to your `build.sbt` file:
```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-rocksdb" % "@VERSION@"
)
```

## Using RocksDB

Use the provided `RocksDB` wrapper:

```scala
import java.nio.charset.StandardCharsets

import zio.rocksdb
import zio.rocksdb.{ RocksDB }

val UTF_8 = StandardCharsets.UTF_8
val key   = "key".getBytes(UTF_8)
val value = "value".getBytes(UTF_8)

val database  = RocksDB.live("/data/state")
val readWrite = RocksDB.put(key, value) *> RocksDB.get(key)
val result    = readWrite.provideCustomLayer(database)
```

## Using TransactionDB

`zio-rocksdb` provides transactional capabilities using RocksDB [Transaction API].

```scala
import java.nio.charset.StandardCharsets

import zio.rocksdb
import zio.rocksdb.{ TransactionDB, Transaction }

val key0   = "key0".getBytes(UTF_8)
val key1   = "key1".getBytes(UTF_8)
val value0 = "value0".getBytes(UTF_8)
val value1 = "value1".getBytes(UTF_8)

val database      = TransactionDB.live("/data/state")
val write0        = Transaction.put(key0, value0)
val write1        = Transaction.put(key1, value1)
val writeTogether = TransactionDB.atomically {
  write0 <&> write1
}
val result        = readWrite.provideCustomLayer(database)
```

[Transaction API]: https://github.com/facebook/rocksdb/wiki/Transactions#transactiondb
