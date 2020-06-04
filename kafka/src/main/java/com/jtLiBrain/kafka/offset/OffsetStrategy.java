package com.jtLiBrain.kafka.offset;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface OffsetStrategy {
    Map<TopicPartition, Long> getLastCommittedOffsets(String topic, String groupId);

    void commitOffsets(String topic, int partition, long offset);
}
