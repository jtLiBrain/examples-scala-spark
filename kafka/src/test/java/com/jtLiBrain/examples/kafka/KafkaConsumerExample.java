package com.jtLiBrain.examples.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.jtLiBrain.examples.kafka.Utils.*;

public class KafkaConsumerExample {
    private String topic = "test";

    private KafkaConsumer<String, String> consumer;

    @Before
    public void before() {
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "3");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "1800000");
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
public void testPoll() {
    int recordCounter = 0;

    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
            recordCounter++;

            System.out.printf(
                    "topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value());
        }

        System.out.println("");
        consumer.commitSync();
        System.out.println("");

        if(recordCounter == 9) {
            return;
        }
    }
}

    @Test
    public void testPartitionsFor() {
        List<PartitionInfo> partitionsForTopic = consumer.partitionsFor(topic);

        partitionsForTopic.forEach(partitionInfo -> {
            PN(partitionInfo.toString());
        });
    }

    /**
     * to get offsets, see usage in {@link kafka.tools.GetOffsetShell}
     */
    @Test
    public void testBeginningOffsets() {
        // TODO
        Collection<TopicPartition> partitions = null;
        consumer.beginningOffsets(partitions);
    }
}
