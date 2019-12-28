[![Release Artifacts][Badge-SonatypeReleases]][Link-SonatypeReleases]

# zio-rocksdb

A ZIO-based interface to RocksDB.

## Quickstart

Add the following dependencies to your `build.sbt` file:
```scala
libraryDependencies ++= Seq(
  "dev.zio" %% "zio-streams" % "1.0.0-RC17",
  "dev.zio" %% "zio-rocksdb" % "<version>"
)
```

Use the provided `RocksDB` wrapper:
```scala
import zio.rocksdb.RocksDB

val result: Task[Option[Array[Byte]]] = 
  RocksDB.open("/data/state").use { db =>
    val key   = "key".getBytes(UTF_8)
    val value = "value".getBytes(UTF_8)
  
    for {
      _      <- db.put(key, value)
      result <- db.get(key)
    } yield result
  }
```

If you need a method which is not wrapped by the library, you can
access the underlying `org.rocksdb.RocksDB` instance using the
`RocksDB#db` field. Make sure to wrap all method calls with `zio.Task`!

## Getting help

Join us on the [ZIO Discord server](https://discord.gg/2ccFBr4).

## Legal

Copyright 2019 Itamar Ravid and the zio-kafka contributors. All rights reserved.

[Link-SonatypeReleases]: https://oss.sonatype.org/content/repositories/releases/dev/zio/zio-rocksdb_2.12/ "Sonatype Releases"
[Badge-SonatypeReleases]: https://img.shields.io/nexus/r/https/oss.sonatype.org/dev.zio/zio-rocksdb_2.12.svg "Sonatype Releases"
