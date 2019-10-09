package com.jtLiBrain.examples.spark.streaming.offset

import kafka.utils.ZkUtils

class OffsetEntryPoint {

  def getLastOffsets(topic: String): Unit = { 
    val zkUrl: String = null
    val sessionTimeout: Int = 0
    val connectionTimeout: Int = 0

    val (zkClient, zkConnection) = ZkUtils.createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeout)

    val zkUtils = new ZkUtils(zkClient, zkConnection, false)

    // current number of partitions from Zookeeper
    val zkNumberOfPartitionsForTopic = zkUtils.getPartitionsForTopics(Seq(topic))
                                              .get(topic).toList.head.size

    val persistedNumberOfPartitionsForTopic = 0
    
   }  

  def commitOffsets(): Unit = {  

  }

}
