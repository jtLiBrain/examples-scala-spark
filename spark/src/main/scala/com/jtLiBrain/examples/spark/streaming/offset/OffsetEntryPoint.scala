package com.jtLiBrain.examples.spark.streaming.offset

import kafka.utils.ZkUtils
import org.apache.kafka.common.TopicPartition

class OffsetEntryPoint {

  def getLastOffsets(topic: String): Map[TopicPartition, Long] = { 
    val zkUrl: String = null
    val sessionTimeout: Int = 0
    val connectionTimeout: Int = 0

    val (zkClient, zkConnection) = ZkUtils.createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeout)

    val zkUtils = new ZkUtils(zkClient, zkConnection, false)

    // current number of partitions from Zookeeper
    val zkNumberOfPartitionsForTopic = zkUtils.getPartitionsForTopics(Seq(topic))
                                              .get(topic).toList.head.size

    val persistedNumberOfPartitionsForTopic = 0

    // TODO the logic get the number of partitions from external storage

    val fromOffsets = collection.mutable.Map[TopicPartition, Long]()

    if(persistedNumberOfPartitionsForTopic == 0) {
      // streaming job is started for first run
      for (partition <- 0 to zkNumberOfPartitionsForTopic-1)
        fromOffsets += (new TopicPartition(topic, partition) -> 0)
    } else if(zkNumberOfPartitionsForTopic > persistedNumberOfPartitionsForTopic) {
      // streaming job is restarted and the number of partitions in the topic was increased

      // for the original partitions
      for (partition <- 0 to persistedNumberOfPartitionsForTopic-1) {
        val fromOffset: Long = 0 // TODO
        fromOffsets += (new TopicPartition(topic, partition) -> fromOffset.toLong)
      }

      // for the new increased partitions
      for (partition <- persistedNumberOfPartitionsForTopic to zkNumberOfPartitionsForTopic-1){
        fromOffsets += (new TopicPartition(topic, partition) -> 0)
      }
    } else {
      // streaming job is restarted and there are no changes to the number of partitions in the topic
      for (partition <- 0 to persistedNumberOfPartitionsForTopic-1 ) {
        val fromOffset: Long = 0 // TODO
        fromOffsets += (new TopicPartition(topic, partition) -> fromOffset.toLong)
      }
    }

    fromOffsets.toMap
   }  

  def commitOffsets(topic: String): Unit = {  

  }

}
