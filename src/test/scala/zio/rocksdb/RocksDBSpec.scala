package zio.rocksdb

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{ Files, Path }

import org.rocksdb.Options
import zio.{ Managed, RIO, Task, UIO }
import zio.rocksdb
import zio.test._
import zio.test.Assertion._

import scala.jdk.CollectionConverters._

object RocksDBSpec extends DefaultRunnableSpec {
  override def spec = suite("RocksDB")(
    testM("get/put") {
      val key   = "key".getBytes(UTF_8)
      val value = "value".getBytes(UTF_8)

      val test =
        for {
          _      <- rocksdb.put(key, value)
          result <- rocksdb.get(key)
        } yield assert(result)(isSome(equalTo(value)))

      test.provideManaged(tempDB)
    },
    testM("delete") {
      val key   = "key".getBytes(UTF_8)
      val value = "value".getBytes(UTF_8)

      val test =
        for {
          _      <- rocksdb.put(key, value)
          before <- rocksdb.get(key)
          _      <- rocksdb.delete(key)
          after  <- rocksdb.get(key)
        } yield assert(before)(isSome(equalTo(value))) && assert(after)(isNone)

      test.provideManaged(tempDB)
    },
    testM("newIterator") {
      val data = (1 to 10).map(i => (s"key$i", s"value$i")).toList

      val test =
        for {
          _          <- RIO.foreach(data) { case (k, v) => rocksdb.put(k.getBytes(UTF_8), v.getBytes(UTF_8)) }
          results    <- rocksdb.newIterator.runCollect
          resultsStr = results.map { case (k, v) => new String(k, UTF_8) -> new String(v, UTF_8) }
        } yield assert(resultsStr)(hasSameElements(data))

      test.provideManaged(tempDB)
    }
  )

  def tempDB: Managed[Throwable, RocksDB] =
    tempDir.flatMap { dir =>
      val opts = new Options().setCreateIfMissing(true)
      RocksDB.Live.open(opts, dir.toAbsolutePath.toString)
    }

  private def tempDir: Managed[Throwable, Path] =
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
}
