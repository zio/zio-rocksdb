package zio.rocksdb

object Atomically {
  object TransactionOnly { implicit val transactionOnly: TransactionOnly = this }
  type TransactionOnly = TransactionOnly.type
  object TransactionWithSomething { implicit val transactionWithSomething: TransactionWithSomething = this }
  type TransactionWithSomething = TransactionWithSomething.type
}
