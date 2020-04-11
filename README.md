[![Release Artifacts][Badge-SonatypeReleases]][Link-SonatypeReleases]

# zio-rocksdb

A ZIO-based interface to RocksDB.

## Table of Content
   - [Quick Start](#quick-start)   
   - [Using Transactions](#using-transactions)
   - [Getting Help](#getting-help)
   - [Legal](#legal)

## Quick Start

Add the following dependencies to your `build.sbt` file:
```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-rocksdb" % "<version>"
)
```

Use the provided `RocksDB` wrapper:
```scala
import java.nio.charset.StandardCharsets

import zio.ZLayer
import zio.rocksdb
import zio.rocksdb.{ Live, RocksDB }

val key   = "key".getBytes(StandardCharsets.UTF_8)
val value = "value".getBytes(StandardCharsets.UTF_8)

val database  = ZLayer.fromManaged(Live.open("/data/state"))
val readWrite = rocksdb.put(key, value) *> rocksdb.get(key)
val result    = readWrite.provideCustomLayer(database)
```

## Using Transactions

```scala
import java.nio.charset.StandardCharsets

import zio.ZLayer
import zio.rocksdb.{transaction}

val key   = "key".getBytes(StandardCharsets.UTF_8)
val value = "value".getBytes(StandardCharsets.UTF_8)

val database  = ZLayer.fromManaged(transaction.Live.open("/data/state"))
val readWrite = transaction.atomically(
  transaction.put(key, value) *> transaction.get(key)
) 
val result    = readWrite.provideCustomLayer(database)
```

## Getting Help

Join us on the [ZIO Discord server](https://discord.gg/2ccFBr4).

## Legal

Copyright 2019 Itamar Ravid and the zio-rocksdb contributors. All rights reserved.

[Link-SonatypeReleases]: https://oss.sonatype.org/content/repositories/releases/dev/zio/zio-rocksdb_2.12/ "Sonatype Releases"
[Badge-SonatypeReleases]: https://img.shields.io/nexus/r/https/oss.sonatype.org/dev.zio/zio-rocksdb_2.12.svg "Sonatype Releases"
