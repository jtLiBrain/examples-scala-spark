package com.jtLiBrain.examples.kafka;

import kafka.utils.ZkUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * NOTE: kafka.utils.ZkUtils is no longer used by Kafka and will be removed in a future release.
  * Please use org.apache.kafka.clients.admin.AdminClient instead.
  *
 * 1. kafka.utils.ZkUtils exist in org.apache.kafka:kafka_2.11 dependency library
 */
class ZkUtilsUsage extends FunSuite with BeforeAndAfterAll {
    private var zkUtils: ZkUtils = _

    override def beforeAll(): Unit = {
        val zkUrl: String = "192.168.56.101:2181"
        val sessionTimeout: Int = 30000
        val connectionTimeout: Int = 30000
        val isZkSecurityEnabled: Boolean = false

        zkUtils = ZkUtils(zkUrl, sessionTimeout, connectionTimeout, isZkSecurityEnabled)
    }

    override def afterAll(): Unit = {
        zkUtils.close()
    }

    test("execute") {
        /*kafka.admin.TopicCommand
        zkUtils.getPartitionsForTopics()*/
    }
}
