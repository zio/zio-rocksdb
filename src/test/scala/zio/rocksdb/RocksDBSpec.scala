package zio.rocksdb

import java.nio.charset.StandardCharsets.UTF_8

import internal.ManagedPath
import org.rocksdb.Options
import zio._
import zio.test.Assertion._
import zio.test._

object RocksDBSpec extends DefaultRunnableSpec {
  def rocksSuite = List(
    testM("get/put") {
      val key   = "key".getBytes(UTF_8)
      val value = "value".getBytes(UTF_8)

      for {
        _      <- rocksdb.put(key, value)
        result <- rocksdb.get(key)
      } yield assert(result)(isSome(equalTo(value)))
    },
    testM("delete") {
      val key   = "key".getBytes(UTF_8)
      val value = "value".getBytes(UTF_8)

      for {
        _      <- rocksdb.put(key, value)
        before <- rocksdb.get(key)
        _      <- rocksdb.delete(key)
        after  <- rocksdb.get(key)
      } yield assert(before)(isSome(equalTo(value))) && assert(after)(isNone)
    },
    testM("newIterator") {
      val data = (1 to 10).map(i => (s"key$i", s"value$i")).toList

      for {
        _          <- RIO.foreach_(data) { case (k, v) => rocksdb.put(k.getBytes(UTF_8), v.getBytes(UTF_8)) }
        results    <- rocksdb.newIterator.runCollect
        resultsStr = results.map { case (k, v) => new String(k, UTF_8) -> new String(v, UTF_8) }
      } yield assert(resultsStr)(hasSameElements(data))
    }
  )

  override def spec =
    suite("db")(
      suite("rocksdb")(rocksSuite: _*).provideCustomLayerShared(database),
      suite("transactiondb")(rocksSuite: _*).provideCustomLayerShared(tdatabase)
    )

  private val database = ZLayer
    .fromManaged(ManagedPath() >>= { dir =>
      Live.open(new Options().setCreateIfMissing(true), dir.toAbsolutePath.toString)
    })
    .mapError(TestFailure.die)

  private val tdatabase = ZLayer
    .fromManaged(ManagedPath() >>= { dir =>
      transaction.Live.open(new Options().setCreateIfMissing(true), dir.toAbsolutePath.toString).map(_.asRocksDB)
    })
    .mapError(TestFailure.die)
}
