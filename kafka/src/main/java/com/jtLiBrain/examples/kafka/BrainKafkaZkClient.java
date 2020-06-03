package com.jtLiBrain.examples.kafka;

import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.utils.Time;

public class BrainKafkaZkClient {
    private KafkaZkClient kafkaZkClient;
    private AdminZkClient adminZkClient;

    private String zkHost;
    private boolean isSecure = false;
    private int sessionTimeoutMs = 30000;
    private int connectionTimeoutMs = 30000;
    private int  maxInFlightRequests = Integer.MAX_VALUE;
    private Time time = Time.SYSTEM;
    private String metricGroup = "kafka.server";
    private String metricType = "SessionExpireListener";

    public void start() {
        kafkaZkClient = KafkaZkClient.apply(
                zkHost, isSecure, sessionTimeoutMs, connectionTimeoutMs, maxInFlightRequests, time,
                metricGroup, metricType
        );
        adminZkClient = new AdminZkClient(kafkaZkClient);
    }

    public void stop() {

    }
}
