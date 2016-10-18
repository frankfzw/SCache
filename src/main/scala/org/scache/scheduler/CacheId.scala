package org.scache.scheduler

/**
 * Created by frankfzw on 16-9-7.
 */
class CacheId (val appId: String, val shuffleId: Int, val partitionNumber: Int) extends Serializable {
}
