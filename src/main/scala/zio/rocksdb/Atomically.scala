package zio.rocksdb

object Atomically {
  object TransactionOnly { implicit val transactionOnly = this }
  type TransactionOnly = TransactionOnly.type
  object TransactionWithSomething { implicit val transactionWithSomething = this }
  type TransactionWithSomething = TransactionWithSomething.type
}
