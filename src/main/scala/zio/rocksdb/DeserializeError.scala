package zio.rocksdb

package zio.rocksdb

sealed abstract class DeserializeError (val message: String) {
  def offset: Long
}

object DeserializeError {
  def apply(msg: String, ofst: Long): DeserializeError =
    new DeserializeError(msg) {
      val offset = ofst
    }

  def unapply(error: DeserializeError): Option[(String, Long)] =
    Some(error.msg, error.offset)
}