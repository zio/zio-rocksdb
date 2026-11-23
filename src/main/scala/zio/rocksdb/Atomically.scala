package zio.rocksdb

/**
 * Marker types for the `TransactionDB.atomically` overloads. Each overload takes one marker as an implicit
 * parameter, which lets the compiler choose between overloads whose parameter types differ only in the
 * environment type of the program.
 */
object Atomically {

  /**
   * Marker for a program whose environment is only `Transaction`. `atomically` with this marker returns a
   * program whose environment is `Any`.
   */
  object TransactionOnly { implicit val transactionOnly: TransactionOnly = this }

  /**
   * The type of the `TransactionOnly` marker.
   */
  type TransactionOnly = TransactionOnly.type

  /**
   * Marker for a program whose environment is `Transaction` together with other services. `atomically` with
   * this marker keeps those services in the environment of the returned program.
   */
  object TransactionWithSomething { implicit val transactionWithSomething: TransactionWithSomething = this }

  /**
   * The type of the `TransactionWithSomething` marker.
   */
  type TransactionWithSomething = TransactionWithSomething.type
}
