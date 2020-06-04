package com.jtLiBrain.kafka;

import com.jtLiBrain.kafka.consumer.StandaloneConsumer;
import com.jtLiBrain.kafka.offset.OffsetStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StandaloneConsumerSuite {
    private Properties props;
    private StandaloneConsumer standaloneConsumer;
    @Before
    public void before() {
        props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-group20200604-2");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "3");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "1800000");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        standaloneConsumer = new StandaloneConsumer<String, String>(){
            @Override
            public void processRecordBatch(ConsumerRecords<String, String> records, OffsetStrategy offsetStrategy) {
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(
                            "topic = %s, partition = %s, offset = %d, key = %s, value = %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
            }
        };
    }

    @Test
    public void testCommit2Kafka() {
        standaloneConsumer.init(props, true, true, null);
        standaloneConsumer.consume("test", 1000);
    }

    @Test
    public void testWithOffsetStrategy() {
        OffsetStrategy offsetStrategy = new OffsetStrategy(){
            @Override
            public Map<TopicPartition, Long> getLastCommittedOffsets(String topic, String groupId) {
                Map<TopicPartition, Long> map = new HashMap<>();

                map.put(new TopicPartition("test", 0), 2L);
                map.put(new TopicPartition("test", 1), 2L);

                return map;
            }

            @Override
            public void commitOffsets(String topic, int partition, long offset) {
                System.out.printf(
                        "topic = %s, partition = %s, offset = %d\n",
                        topic, partition, offset);
            }
        };


        standaloneConsumer.init(props, false, true, offsetStrategy);
        standaloneConsumer.consume("test", 1000);
    }
}
