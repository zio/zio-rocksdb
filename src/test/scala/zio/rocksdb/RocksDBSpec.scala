package zio.rocksdb

import zio.UIO
import zio.test._
import zio.test.Assertion._

class RocksDBSpec
    extends DefaultRunnableSpec(
      suite("RocksDB")(
        suite("get")(
          testM("default column family")(
            UIO.succeed(assert(true, isFalse))
          )
        )
      )
    )
