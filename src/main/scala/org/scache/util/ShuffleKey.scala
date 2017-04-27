package org.scache.util

/**
  * Created by frankfzw on 16-12-21.
  */

private[scache] object ShuffleKey {
  def fromString(key: String): ShuffleKey = {
    val metas = key.split("_")
    if (metas.size < 3) {
      return ShuffleKey("null", 0, 0)
    }
    return ShuffleKey(metas(0), Integer.parseInt(metas(1)), Integer.parseInt((metas(2))))
  }
}


private[scache] case class ShuffleKey(appName: String, jobId: Int, shuffleId: Int) extends Serializable{
  override def toString(): String = s"${appName}_${jobId}_${shuffleId}"
}
