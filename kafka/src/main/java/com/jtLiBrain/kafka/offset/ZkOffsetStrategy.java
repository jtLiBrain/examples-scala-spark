package com.jtLiBrain.kafka.offset;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class ZkOffsetStrategy implements OffsetStrategy {
    @Override
    public Map<TopicPartition, Long> getLastCommittedOffsets(String topic, String groupId) {
        return null;
    }

    @Override
    public void commitOffsets(String topic, int partition, long offset) {

    }
}
