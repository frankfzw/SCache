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


private[scache] class ReduceStatus(reduceId: Int, hostAddr: String, bak: Array[String]) extends Serializable{
  val id = reduceId
  val host = hostAddr
  val backups = bak
  var size = 0
}


