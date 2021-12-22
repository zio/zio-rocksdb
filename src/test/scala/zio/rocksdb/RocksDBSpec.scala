package zio.rocksdb

import java.nio.charset.StandardCharsets.UTF_8
import org.{ rocksdb => jrocks }
import zio.RIO
import zio.rocksdb.internal.ManagedPath
import zio.test.Assertion._
import zio.test._
import zio.test.ZIOSpecDefault

object RocksDBSpec extends ZIOSpecDefault {
  override def spec = {
    val rocksSuite = suite("RocksDB")(
      test("get/put") {
        val key   = "key".getBytes(UTF_8)
        val value = "value".getBytes(UTF_8)

        for {
          _      <- RocksDB.put(key, value)
          result <- RocksDB.get(key)
        } yield assert(result)(isSome(equalTo(value)))
      },
      test("delete") {
        val key   = "key".getBytes(UTF_8)
        val value = "value".getBytes(UTF_8)

        for {
          _      <- RocksDB.put(key, value)
          before <- RocksDB.get(key)
          _      <- RocksDB.delete(key)
          after  <- RocksDB.get(key)
        } yield assert(before)(isSome(equalTo(value))) && assert(after)(isNone)
      },
      test("newIterator") {
        val data = (1 to 10).map(i => (s"key$i", s"value$i")).toList

        for {
          _          <- RIO.foreachDiscard(data) { case (k, v) => RocksDB.put(k.getBytes(UTF_8), v.getBytes(UTF_8)) }
          results    <- RocksDB.newIterator.runCollect
          resultsStr = results.map { case (k, v) => new String(k, UTF_8) -> new String(v, UTF_8) }
        } yield assert(resultsStr)(hasSameElements(data))
      }
    )

    rocksSuite.provideCustomLayer(database)
  }

  private val database = (for {
    dir <- ManagedPath()
    db <- {
      val opts = new jrocks.Options().setCreateIfMissing(true)
      RocksDB.Live.open(opts, dir.toAbsolutePath.toString)
    }
  } yield db).toLayer.mapError(TestFailure.die)

}
