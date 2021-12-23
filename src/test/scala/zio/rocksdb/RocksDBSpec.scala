package zio.rocksdb

import org.rocksdb.{ ColumnFamilyDescriptor, ColumnFamilyOptions }
import org.{ rocksdb => jrocks }
import zio.rocksdb.internal.ManagedPath
import zio.test.Assertion._
import zio.test._
import zio.{ RIO, ZIO }

import java.nio.charset.StandardCharsets.UTF_8

object RocksDBSpec extends DefaultRunnableSpec {
  override def spec = {
    val rocksSuite = suite("RocksDB")(
      testM("get/put") {
        val key   = "key".getBytes(UTF_8)
        val value = "value".getBytes(UTF_8)

        for {
          _      <- RocksDB.put(key, value)
          result <- RocksDB.get(key)
        } yield assert(result)(isSome(equalTo(value)))
      },
      testM("delete") {
        val key   = "key".getBytes(UTF_8)
        val value = "value".getBytes(UTF_8)

        for {
          _      <- RocksDB.put(key, value)
          before <- RocksDB.get(key)
          _      <- RocksDB.delete(key)
          after  <- RocksDB.get(key)
        } yield assert(before)(isSome(equalTo(value))) && assert(after)(isNone)
      },
      testM("newIterator") {
        val data = (1 to 10).map(i => (s"key$i", s"value$i")).toList

        for {
          _          <- RIO.foreach_(data) { case (k, v) => RocksDB.put(k.getBytes(UTF_8), v.getBytes(UTF_8)) }
          results    <- RocksDB.newIterator.runCollect
          resultsStr = results.map { case (k, v) => new String(k, UTF_8) -> new String(v, UTF_8) }
        } yield assert(resultsStr)(hasSameElements(data))
      },
      testM("ownedColumnFamilyHandles") {
        for {
          cfHandles <- RocksDB.ownedColumnFamilyHandles()
        } yield assert(cfHandles)(equalTo(Nil))
      },
      testM("ownedColumnFamilyHandles- concurrent creation") {
        val zio: ZIO[RocksDB, Throwable, Int] = ZIO
          .foreachPar((0 until 100).toList)(i => {
            val cfDescriptor = new ColumnFamilyDescriptor(s"TestColFam${i.toString}".getBytes())
            RocksDB.createColumnFamily(cfDescriptor)
          })
          .map(_.size)
        assertM(zio)(equalTo(100))
      },
      testM("ownedColumnFamilyHandles - check actual values") {
        val cfDescriptor = new ColumnFamilyDescriptor("TestColFam1".getBytes())
        for {
          _             <- RocksDB.createColumnFamily(cfDescriptor)
          newHandleList <- RocksDB.ownedColumnFamilyHandles()
        } yield assert(newHandleList.head.getName)(equalTo("TestColFam1".getBytes()))
      },
      testM("createColumnHandle") {
        val cfDescriptor = new ColumnFamilyDescriptor("ColFam1".getBytes())
        for {
          newHandleList <- RocksDB.createColumnFamily(cfDescriptor)
        } yield assert(newHandleList.getName)(equalTo("ColFam1".getBytes()))
      },
      testM("createColumnFamilyHandles") {
        val cfDescriptor = List(
          new ColumnFamilyDescriptor("TestColFam1".getBytes()),
          new ColumnFamilyDescriptor("TestColFam2".getBytes()),
          new ColumnFamilyDescriptor("ColFam3".getBytes())
        )
        for {
          newHandleList <- RocksDB.createColumnFamilies(cfDescriptor)
        } yield assert(newHandleList.head.getName)(equalTo("TestColFam1".getBytes()))
      },
      testM("createColumnFamilyHandles") {
        val cfNames = List(
          "TestColFam1".getBytes(),
          "TestColFam2".getBytes(),
          "TestColFam3".getBytes()
        )
        for {
          newHandleList <- RocksDB.createColumnFamilies(new ColumnFamilyOptions(), cfNames)
        } yield assert(newHandleList(1).getName)(equalTo("TestColFam2".getBytes()))
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
