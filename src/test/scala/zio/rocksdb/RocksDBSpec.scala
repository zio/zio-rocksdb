package zio.rocksdb

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{ Files, Path }

import org.rocksdb.Options
import zio.{ Managed, Task, UIO }
import zio.test._
import zio.test.Assertion._

import scala.jdk.CollectionConverters._

object Utils {
  def tempDir: Managed[Throwable, Path] =
    Task(Files.createTempDirectory("zio-rocksdb")).toManaged { path =>
      UIO {
        Files
          .walk(path)
          .iterator()
          .asScala
          .toList
          .map(_.toFile)
          .sorted((o1: File, o2: File) => -o1.compareTo(o2))
          .foreach(_.delete)
      }
    }

  def tempDB: Managed[Throwable, RocksDB] = {
    val opts = new Options().setCreateIfMissing(true)

    Utils.tempDir
      .flatMap(p => RocksDB.open(opts, p.toAbsolutePath.toString))

  }
}

object RocksDBSpec
    extends DefaultRunnableSpec(
      suite("RocksDB")(
        testM("get/put") {
          val key   = "key".getBytes(UTF_8)
          val value = "value".getBytes(UTF_8)

          Utils.tempDB.use { db =>
            for {
              _      <- db.put(key, value)
              result <- db.get(key)
            } yield assert(result, isSome(equalTo(value)))
          }
        },
        testM("delete") {
          val key   = "key".getBytes(UTF_8)
          val value = "value".getBytes(UTF_8)

          Utils.tempDB.use { db =>
            for {
              _      <- db.put(key, value)
              before <- db.get(key)
              _      <- db.delete(key)
              after  <- db.get(key)
            } yield assert(before, isSome(equalTo(value))) && assert(after, isNone)
          }
        },
        testM("newIterator") {
          Utils.tempDB.use { db =>
            val data = (1 to 10).map(i => (s"key$i", s"value$i")).toList
            for {
              _ <- Task.foreach(data) {
                    case (k, v) => db.put(k.getBytes(UTF_8), v.getBytes(UTF_8))
                  }
              results <- db.newIterator.runCollect
              resultsStr = results.map {
                case (k, v) => new String(k, UTF_8) -> new String(v, UTF_8)
              }
            } yield assert(resultsStr, hasSameElements(data))
          }
        }
      )
    )
