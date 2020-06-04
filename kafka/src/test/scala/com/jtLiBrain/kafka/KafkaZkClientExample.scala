package com.jtLiBrain.kafka

import com.typesafe.scalalogging.LazyLogging
import kafka.server.ConfigType
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.{Seq, immutable}
import scala.collection.JavaConverters._

/**
  * NOTE:
  *
  * 1. kafka.zk.KafkaZkClient exist in org.apache.kafka:kafka_2.11 dependency library
  * 2. usage about kafka.zk.KafkaZkClient see code in kafka.admin.TopicCommand
  */
class KafkaZkClientExample extends FunSuite with BeforeAndAfterAll with LazyLogging {
  private var kafkaZkClient: KafkaZkClient = _
  private var adminZkClient: AdminZkClient = _

  private val topic = "test"

  override def beforeAll(): Unit = {
    val connectString = "192.168.56.101:2181"
    val isSecure = false
    val sessionTimeoutMs = 30000
    val connectionTimeoutMs = 30000
    val maxInFlightRequests = Int.MaxValue
    val time = Time.SYSTEM

    kafkaZkClient = KafkaZkClient(
      connectString, isSecure, sessionTimeoutMs, connectionTimeoutMs, maxInFlightRequests, time
    )

    adminZkClient = new AdminZkClient(kafkaZkClient)
  }

  override def afterAll(): Unit = {
    kafkaZkClient.close()
  }

  /**
    * read from /brokers/topics in zookeeper
    */
  test("getAllTopicsInCluster") {
    kafkaZkClient.getAllTopicsInCluster.foreach(println(_))
  }

  /**
    * read from /brokers/topics/{topic} in zookeeper
    */
  test("getPartitionAssignmentForTopics") {
    val configs = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic).asScala
    val markedForDeletion = kafkaZkClient.isTopicMarkedForDeletion(topic)

    kafkaZkClient.getPartitionAssignmentForTopics(immutable.Set(topic)).get(topic) match {
      case Some(topicPartitionAssignment) =>
        val numPartitions = topicPartitionAssignment.size
        val replicationFactor = topicPartitionAssignment.head._2.size
        val configsAsString = configs.map { case (k, v) => s"$k=$v" }.mkString(",")
        val markedForDeletionString = if (markedForDeletion) "\tMarkedForDeletion:true" else ""

        println("Topic:%s\tPartitionCount:%d\tReplicationFactor:%d\tConfigs:%s%s"
          .format(topic, numPartitions, replicationFactor, configsAsString, markedForDeletionString))

        val sortedPartitions = topicPartitionAssignment.toSeq.sortBy(_._1)
        for ((partitionId, assignedReplicas) <- sortedPartitions) {
          val leaderIsrEpoch = kafkaZkClient.getTopicPartitionState(new TopicPartition(topic, partitionId))
          val inSyncReplicas = if (leaderIsrEpoch.isEmpty) Seq.empty[Int] else leaderIsrEpoch.get.leaderAndIsr.isr
          val leader = if (leaderIsrEpoch.isEmpty) None else Option(leaderIsrEpoch.get.leaderAndIsr.leader)

          val markedForDeletionString =
            if (markedForDeletion) "\tMarkedForDeletion: true" else ""

          print("\tTopic: " + topic)
          print("\tPartition: " + partitionId)
          print("\tLeader: " + (if(leader.isDefined) leader.get else "none"))
          print("\tReplicas: " + assignedReplicas.mkString(","))
          print("\tIsr: " + inSyncReplicas.mkString(","))
          print(markedForDeletionString)
          println()
        }
      case None =>
        println("Topic " + topic + " doesn't exist!")
    }
  }

  /**
    * get data from /consumers/{group-name}/offsets/{topic-name}/{partition-num} in Zookeeper
    */
  test("getConsumerOffset") {
    val consumerGroup = "test-group"

    val offsets = collection.mutable.Buffer[(String, Int, Long)]()

    kafkaZkClient.getPartitionAssignmentForTopics(immutable.Set(topic)).get(topic) match {
      case Some(topicPartitionAssignment) =>
        val sortedPartitions = topicPartitionAssignment.toSeq.sortBy(_._1)
        for ((partitionId, assignedReplicas) <- sortedPartitions) {

          val topicPartition = new TopicPartition(topic, partitionId)

          kafkaZkClient.getConsumerOffset(consumerGroup, topicPartition) match {
            case Some(offset) => offsets += ((topic, partitionId, offset))
            case None => offsets += ((topic, partitionId, -1)) // just for display
          }
        }
      case None =>
        logger.warn(s"No partitions for the topic:$topic")
    }

    offsets.foreach{ offset =>
      print("\tTopic: " + offset._1)
      print("\tPartition: " + offset._2)
      print("\tOffset: " + offset._3)
      println()
    }
  }

  /**
    * set data into /consumers/{group-name}/offsets/{topic-name}/{partition-num} in Zookeeper
    */
  test("setOrCreateConsumerOffset") {
    val topic = "test2"
    val consumerGroup = "test2-group2"
    val partitionId = 0
    val offset = 2

    val topicPartition = new TopicPartition(topic, partitionId)

    kafkaZkClient.setOrCreateConsumerOffset(consumerGroup, topicPartition, offset)
  }

  test("getConsumerOffset2") {
    val topic = "test2"
    val consumerGroup = "test2-group2"
    val partitionId = 0

    val topicPartition = new TopicPartition(topic, partitionId)

    kafkaZkClient.getConsumerOffset(consumerGroup, topicPartition).foreach(println(_))
  }
}
