package zio.rocksdb.transaction

import java.nio.charset.StandardCharsets.UTF_8

import internal.ManagedPath
import org.{ rocksdb => jrocks }
import zio.duration._
import zio.test.Assertion.{ equalTo, isSome }
import zio.test.TestAspect._
import zio.test._
import zio.{ console, ZIO, ZLayer }

import scala.language.postfixOps

object TransactionDBSpec extends DefaultRunnableSpec {

  override def spec =
    suite("TransactionDBSpec")(
      testM("get/put") {
        val key   = "key".getBytes(UTF_8)
        val value = "value".getBytes(UTF_8)
        for {
          result <- atomically(for {
                     _      <- put(key, value)
                     result <- get(key)
                   } yield result)
        } yield assert(result)(isSome(equalTo(value)))
      },
      testM("get/put with Console") {
        val key   = "key".getBytes(UTF_8)
        val value = "value".getBytes(UTF_8)

        for {
          result <- atomically(for {
                     _      <- put(key, value)
                     result <- get(key)
                     _      <- console.putStrLn(result.toString)
                   } yield result)
        } yield assert(result)(isSome(equalTo(value)))
      },
      testM("concurrent updates to the same key") {
        val count    = 10
        val key      = "COUNT".getBytes(UTF_8)
        val expected = isSome(equalTo(count))

        for {
          _ <- atomically(put(key, 0.toString.getBytes(UTF_8)))
          _ <- concurrent(count) {
                atomically(getForUpdate(key, exclusive = true) >>= { iCount =>
                  put(key, iCount.map(bytesToInt).map(_ + 1).getOrElse(-1).toString.getBytes(UTF_8))
                })
              }
          actual <- atomically(get(key))
        } yield assert(actual.map(bytesToInt))(expected)
      }
    ).provideCustomLayerShared(dbLayer) @@ timeout(1 second)

  private def bytesToInt(bytes: Array[Byte]): Int                                = new String(bytes, UTF_8).toInt
  private def concurrent[R, E, A](n: Int)(zio: ZIO[R, E, A]): ZIO[R, E, List[A]] = ZIO.foreachPar(0 until n)(_ => zio)
  private def dbLayer: ZLayer[Any, TestFailure[Nothing], TransactionDB] =
    ZLayer
      .fromManaged(ManagedPath() >>= { dir =>
        ZTransactionDB.open(new jrocks.Options().setCreateIfMissing(true), dir.toAbsolutePath.toString)
      })
      .mapError(TestFailure.die)

}
