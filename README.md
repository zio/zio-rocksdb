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
import zio.rocksdb
import zio.rocksdb.RocksDB
import java.nio.charset.StandardCharsets

val key   = "key".getBytes(StandardCharsets.UTF_8)
val value = "value".getBytes(StandardCharsets.UTF_8)

val readWrite =
  for {
    _      <- rocksdb.put(key, value)
    result <- rocksdb.get(key)
  } yield result

val result: Task[Option[Array[Byte]]] =
  readWrite.provideManaged(RocksDB.Live.open("/data/state"))
```

## Getting help

Join us on the [ZIO Discord server](https://discord.gg/2ccFBr4).

## Legal

Copyright 2019 Itamar Ravid and the zio-rocksdb contributors. All rights reserved.

[Link-SonatypeReleases]: https://oss.sonatype.org/content/repositories/releases/dev/zio/zio-rocksdb_2.12/ "Sonatype Releases"
[Badge-SonatypeReleases]: https://img.shields.io/nexus/r/https/oss.sonatype.org/dev.zio/zio-rocksdb_2.12.svg "Sonatype Releases"
