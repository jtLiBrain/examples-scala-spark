package com.jtLiBrain.kafka

import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.utils.Time

class BrainKafkaZkClient {
  private var kafkaZkClient: KafkaZkClient = _
  private var adminZkClient: AdminZkClient = _

  def start(zkHost: String,
            isSecure: Boolean = false,
            sessionTimeoutMs: Int = 30000,
            connectionTimeoutMs: Int = 30000,
            maxInFlightRequests: Int = Int.MaxValue,
            time: Time = Time.SYSTEM,
            metricGroup: String = "kafka.server",
            metricType: String = "SessionExpireListener"
           ): Unit = {
    kafkaZkClient = KafkaZkClient(
      zkHost, isSecure, sessionTimeoutMs, connectionTimeoutMs, maxInFlightRequests, time
    )

    adminZkClient = new AdminZkClient(kafkaZkClient)
  }

  def close(): Unit = {
    if(kafkaZkClient != null) kafkaZkClient.close()
  }
}
