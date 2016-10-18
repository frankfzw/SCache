/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.scache.scheduler



import org.scache.storage.{BlockManagerId, BlockUpdatedInfo}

trait ScacheListenerEvent {
  /* Whether output this event to the event log */
  protected[scache] def logEvent: Boolean = true
}



case class ScacheListenerBlockManagerAdded(time: Long, blockManagerId: BlockManagerId, maxMem: Long)
  extends ScacheListenerEvent

case class ScacheListenerBlockManagerRemoved(time: Long, blockManagerId: BlockManagerId)
  extends ScacheListenerEvent

case class ScacheListenerUnpersistRDD(rddId: Int) extends ScacheListenerEvent

case class ScacheListenerBlockUpdated(blockUpdatedInfo: BlockUpdatedInfo) extends ScacheListenerEvent


/**
 * An internal class that describes the metadata of an event log.
 * This event is not meant to be posted to listeners downstream.
 */
private[scache] case class ScacheListenerLogStart(scacheVersion: String) extends ScacheListenerEvent




/**
 * Interface for listening to events from the Scache scheduler. Most applications should probably
 * extend ScacheListener or ScacheFirehoseListener directly, rather than implementing this class.
 *
 * Note that this is an internal interface which might change in different Scache releases.
 */
private[scache] trait ScacheListenerInterface {


  /**
   * Called when a new block manager has joined
   */
  def onBlockManagerAdded(blockManagerAdded: ScacheListenerBlockManagerAdded): Unit

  /**
   * Called when an existing block manager has been removed
   */
  def onBlockManagerRemoved(blockManagerRemoved: ScacheListenerBlockManagerRemoved): Unit

  /**
   * Called when an RDD is manually unpersisted by the application
   */
  def onUnpersistRDD(unpersistRDD: ScacheListenerUnpersistRDD): Unit

  /**
   * Called when the driver receives a block update info.
   */
  def onBlockUpdated(blockUpdated: ScacheListenerBlockUpdated): Unit

  /**
   * Called when other events like SQL-specific events are posted.
   */
  def onOtherEvent(event: ScacheListenerEvent): Unit
}


/**
 * :: DeveloperApi ::
 * A default implementation for [[ScacheListenerInterface]] that has no-op implementations for
 * all callbacks.
 *
 * Note that this is an internal interface which might change in different Scache releases.
 */
abstract class ScacheListener extends ScacheListenerInterface {

  override def onBlockManagerAdded(blockManagerAdded: ScacheListenerBlockManagerAdded): Unit = { }

  override def onBlockManagerRemoved(
      blockManagerRemoved: ScacheListenerBlockManagerRemoved): Unit = { }

  override def onUnpersistRDD(unpersistRDD: ScacheListenerUnpersistRDD): Unit = { }

  override def onBlockUpdated(blockUpdated: ScacheListenerBlockUpdated): Unit = { }

  override def onOtherEvent(event: ScacheListenerEvent): Unit = { }
}
