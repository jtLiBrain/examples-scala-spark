package com.jtLiBrain.examples.kafka


import java.util.Properties

import kafka.admin.AdminClient
import org.apache.kafka.clients.CommonClientConfigs
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * NOTE:
  *
  * 1. kafka.admin.AdminClient exist in org.apache.kafka:kafka_2.11 dependency library
  * 2. org.apache.kafka.clients.admin.AdminClient exist in org.apache.kafka:kafka-clients dependency library
  *
  * 3. usage about kafka.admin.AdminClient see code in kafka.admin.ConsumerGroupCommand
  */
class AdminClientUsage extends FunSuite with BeforeAndAfterAll {
  private var adminClient: AdminClient = _

  private val consumerGroup = "test-group5"

  override def beforeAll(): Unit = {
    val props = new Properties()

    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    adminClient = AdminClient.create(props)
  }

  override def afterAll(): Unit = {
    adminClient.close()
  }

  test("describeConsumerGroup") {
    val consumerGroupSummary = adminClient.describeConsumerGroup(consumerGroup, 5000)
  }

  /**
    * use topics named '__consumer_offsets' in Kafka
    */
  test("listGroupOffsets") {
    val offsets = adminClient.listGroupOffsets(consumerGroup)

    val sortedOffsets = offsets.toSeq.sortBy(_._1.partition())
    for ((topicPartition, offset) <- sortedOffsets) {
      print("\tTopic: " + topicPartition.topic())
      print("\tPartition: " + topicPartition.partition())
      print("\tOffsets: " + offset)
      println()
    }
  }
}
