package com.jtLiBrain.kafka.consumer;

import com.jtLiBrain.kafka.offset.OffsetStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class StandaloneConsumer<K, V> extends AbstractConsumer<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(StandaloneConsumer.class);

    /**
     * process records and commit the offsets for each batch
     * @param topic
     * @param timeoutMS
     */
    public void consume(String topic, long timeoutMS) {
        Map<TopicPartition, Long> offsetMap = null;
        if(!super.autoCommit && !commit2Kafka) {
            offsetMap = super.offsetStrategy.getLastCommittedOffsets(topic, super.groupId);
        }

        List<TopicPartition> partitions = new ArrayList<TopicPartition>();
        // ask the cluster for the partitions available in the topic
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos != null) {
            for (PartitionInfo partition : partitionInfos) {
                partitions.add(new TopicPartition(partition.topic(), partition.partition()));
            }

            consumer.assign(partitions);

            if(!super.autoCommit && !commit2Kafka) {
                for (TopicPartition partition : partitions) {
                    long offset = offsetMap.getOrDefault(partition, 0L); // for the new added partitions, use 0 as its offset
                    consumer.seek(partition, offset);
                }
            }

            try {
                while (true) {
                    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(timeoutMS));

                    processRecordBatch(records, offsetStrategy);

                    if(!super.autoCommit && commit2Kafka) {
                        if(super.commit2KafkaSync)
                            consumer.commitSync();
                        else
                            consumer.commitAsync();
                    }
                }
            } catch (WakeupException e) {
                // ignore for shutdown
            } catch (Exception e) {
                logger.error("Error is encountered", e);
            } finally {
                super.close();
            }
        }
    }

    /**
     *
     * @param records
     * @param offsetStrategy
     */
    public abstract void processRecordBatch(ConsumerRecords<K, V> records, OffsetStrategy offsetStrategy);
}
