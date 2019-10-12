package com.jtLiBrain.examples.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.jtLiBrain.examples.kafka.Utils.*;

public class KafkaConsumerSuite {
    private String topic = "test";

    private KafkaConsumer<String, String> consumer;

    @Before
    public void before() {
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group5");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(topic));
    }

    @After
    public void after() {
        if(consumer != null)
            consumer.close();
    }

    @Test
    public void test() {
        List<PartitionInfo> partitionsForTopic = consumer.partitionsFor(topic);

        partitionsForTopic.forEach(partitionInfo -> {
            PN(partitionInfo.toString());
        });
    }

}
