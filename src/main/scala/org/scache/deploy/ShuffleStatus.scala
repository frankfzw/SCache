package org.scache.deploy

/**
  * Created by frankfzw on 16-12-20.
  */
class ShuffleStatus(id: Int, numMap: Int, numReduce: Int) extends Serializable{
  val shffleId = id
  val mapTaskNum = numMap
  val reduceTaskNum = numReduce
  val reduceArray = new Array[ReduceStatus](numReduce)
}


private[scache] class ReduceStatus(
  val id: Int,
  val host: String,
  val backups: Array[String],
  val numMap: Int) extends Serializable


