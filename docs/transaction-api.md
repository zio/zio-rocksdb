---
id: transaction-api
title: "Transaction API"
---

`zio-rocksdb` provides transactional capabilities using RocksDB [Transaction API].

```scala mdoc:compile-only
import java.nio.charset.StandardCharsets._
import zio.rocksdb.{Transaction, TransactionDB}

val key0   = "key0".getBytes(UTF_8)
val key1   = "key1".getBytes(UTF_8)
val value0 = "value0".getBytes(UTF_8)
val value1 = "value1".getBytes(UTF_8)

val database      = TransactionDB.live(new org.rocksdb.Options(), "/data/state")
val write0        = Transaction.put(key0, value0)
val write1        = Transaction.put(key1, value1)
val writeTogether = TransactionDB.atomically {
  write0 <&> write1
}
val result        = writeTogether.provideLayer(database)
```

[Transaction API]: https://github.com/facebook/rocksdb/wiki/Transactions#transactiondb
