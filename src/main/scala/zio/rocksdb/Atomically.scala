package zio.rocksdb

/**
 * Marker types for the `TransactionDB.atomically` overloads. Each overload takes one marker as an implicit
 * parameter. The markers give overloads that differ only in the environment type of the program different
 * erased signatures, so that all of them can be defined.
 */
object Atomically {

  /**
   * Marker for a program whose environment is only `Transaction`. The `atomically` method of a `TransactionDB`
   * instance with this marker returns a program whose environment is `Any`. The accessor on the `TransactionDB`
   * companion object returns a program that needs only a `TransactionDB`.
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
