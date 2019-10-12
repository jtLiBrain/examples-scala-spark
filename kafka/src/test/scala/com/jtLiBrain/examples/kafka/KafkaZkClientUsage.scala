package com.jtLiBrain.examples.kafka

import kafka.zk.KafkaZkClient
import org.apache.kafka.common.utils.Time
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * NOTE:
  *
  * 1. kafka.zk.KafkaZkClient exist in org.apache.kafka:kafka_2.11 dependency library
  * 2. usage about kafka.zk.KafkaZkClient see code in kafka.admin.TopicCommand
  */
class KafkaZkClientUsage extends FunSuite with BeforeAndAfterAll {
  private var kafkaZkClient: KafkaZkClient = _

  override def beforeAll(): Unit = {
    val connectString = ""
    val isSecure = false
    val sessionTimeoutMs = 30000
    val connectionTimeoutMs = 30000
    val maxInFlightRequests = Int.MaxValue
    val time = Time.SYSTEM

    kafkaZkClient = KafkaZkClient(
      connectString, isSecure, sessionTimeoutMs, connectionTimeoutMs, maxInFlightRequests, time
    )
  }

  override def afterAll(): Unit = {
    kafkaZkClient.close()
  }

  test("") {

  }
}
