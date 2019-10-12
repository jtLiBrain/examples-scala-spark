package com.jtLiBrain.examples.kafka


import java.util.Properties

import kafka.admin.AdminClient
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

  override def beforeAll(): Unit = {
    val props:Properties = null
    adminClient = AdminClient.create(props)
  }

  override def afterAll(): Unit = {
    adminClient.close()
  }
}
