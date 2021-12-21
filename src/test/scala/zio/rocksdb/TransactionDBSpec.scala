package zio.rocksdb

import org.{ rocksdb => jrocks }
import zio.duration._
import zio.rocksdb.internal.ManagedPath
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.{ BuildFrom, RIO, ZIO }

import java.nio.charset.StandardCharsets.UTF_8
import scala.language.postfixOps

object TransactionDBSpec extends DefaultRunnableSpec {
  override def spec = {
    val rocksSuite = suite("TransactionDB")(
      testM("get/put") {
        val key   = "key".getBytes(UTF_8)
        val value = "value".getBytes(UTF_8)

        for {
          _      <- TransactionDB.put(key, value)
          result <- TransactionDB.get(key)
        } yield assert(result)(isSome(equalTo(value)))
      },
      testM("delete") {
        val key   = "key".getBytes(UTF_8)
        val value = "value".getBytes(UTF_8)

        for {
          _      <- TransactionDB.put(key, value)
          before <- TransactionDB.get(key)
          _      <- TransactionDB.delete(key)
          after  <- TransactionDB.get(key)
        } yield assert(before)(isSome(equalTo(value))) && assert(after)(isNone)
      },
      testM("newIterator") {
        val data = (1 to 10).map(i => (s"key$i", s"value$i")).toList

        for {
          _          <- RIO.foreach_(data) { case (k, v) => TransactionDB.put(k.getBytes(UTF_8), v.getBytes(UTF_8)) }
          results    <- TransactionDB.newIterator.runCollect
          resultsStr = results.map { case (k, v) => new String(k, UTF_8) -> new String(v, UTF_8) }
        } yield assert(resultsStr)(hasSameElements(data))
      },
      testM("get/put with Console") {
        val key   = "key".getBytes(UTF_8)
        val value = "value".getBytes(UTF_8)

        for {
          result <- TransactionDB.atomically(for {
                     _      <- Transaction.put(key, value)
                     result <- Transaction.get(key)
                   } yield result)
        } yield assert(result)(isSome(equalTo(value)))
      },
      testM("concurrent updates to the same key") {
        val count    = 10
        val key      = "COUNT".getBytes(UTF_8)
        val expected = isSome(equalTo(count))

        for {
          _ <- TransactionDB.put(key, 0.toString.getBytes(UTF_8))
          _ <- concurrent(count) {
                TransactionDB.atomically {
                  Transaction.getForUpdate(key, exclusive = true) >>= { iCount =>
                    Transaction.put(key, iCount.map(bytesToInt).map(_ + 1).getOrElse(-1).toString.getBytes(UTF_8))
                  }
                }
              }
          actual <- TransactionDB.get(key)
        } yield assert(actual.map(bytesToInt))(expected)
      },
      testM("thread safety inside transaction") {
        checkM(byteArray, byteArray) { (b1, b2) =>
          for {
            _ <- TransactionDB.atomically {
                  Transaction.put(b1, b2) <&> Transaction.put(b2, b1)
                }
          } yield assertCompletes
        }
      } @@ nonFlaky(10)
    ) @@ timeout(5 second)

    rocksSuite.provideCustomLayer(database)
  }
  private def bytesToInt(bytes: Array[Byte]): Int = new String(bytes, UTF_8).toInt
  private def concurrent[R, E, A](
    n: Int
  )(zio: ZIO[R, E, A])(implicit bf: BuildFrom[List[Int], A, List[A]]): ZIO[R, E, List[A]] =
    ZIO.foreachPar((0 until n).toList)(_ => zio)

  private val database = (for {
    dir <- ManagedPath()
    db <- {
      val opts = new jrocks.Options().setCreateIfMissing(true)
      TransactionDB.Live.open(opts, dir.toAbsolutePath.toString)
    }
  } yield db).toLayer.mapError(TestFailure.die)

  private def byteArray = Gen.listOf(Gen.anyByte).map(_.toArray)
}
