package com.jtLiBrain.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminClientJavaExample {
    private AdminClient adminClient;

    @Before
    public void before() {
        Properties props = new Properties();

        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");

        adminClient = AdminClient.create(props);
    }

    @After
    public void after() {
        adminClient.close();
    }

    /**
     * zk中下面两个节点都会创建相应的数据
     * /brokers/topics
     * /config/topics
     */
    @Test
    public void testCreateTopics() {
        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        newTopics.add(new NewTopic("test-topics-1", 3, (short) 1));
        adminClient.createTopics(newTopics);
    }

    /**
     * 使用 Kafka中的 __consumer_offsets 主题
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testListConsumerGroupOffsets() throws ExecutionException, InterruptedException {
        String groupId = "test-group";
        ListConsumerGroupOffsetsResult groupOffsetsResult = adminClient.listConsumerGroupOffsets(groupId);

        Map<TopicPartition, OffsetAndMetadata> map = groupOffsetsResult.partitionsToOffsetAndMetadata().get();
        map.forEach((partition, offsetAndMetadata) -> {
            System.out.printf(
                    "topic = %s, partition = %s, offset = %d\n",
                    partition.topic(), partition.partition(),
                    offsetAndMetadata.offset());
        });
    }
}
