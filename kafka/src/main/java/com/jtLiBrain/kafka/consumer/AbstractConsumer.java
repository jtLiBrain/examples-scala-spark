package com.jtLiBrain.kafka.consumer;

import com.jtLiBrain.kafka.offset.OffsetStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public abstract class AbstractConsumer<K, V> {
    protected Properties props;
    protected KafkaConsumer<K, V> consumer;
    protected OffsetStrategy offsetStrategy;

    protected boolean autoCommit = true;
    protected boolean commit2Kafka = false;
    protected boolean commit2KafkaSync = false;
    protected String groupId;

    /**
     *
     * @param props
     * @param commit2Kafka
     * @param commit2KafkaSync
     * @param offsetStrategy
     */
    public void init(Properties props, boolean commit2Kafka, boolean commit2KafkaSync, OffsetStrategy offsetStrategy) {
        this.props = props;
        this.offsetStrategy = offsetStrategy;
        this.commit2Kafka = commit2Kafka;
        this.commit2KafkaSync = commit2KafkaSync;

        String autoCommitConf = props.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        if(autoCommitConf != null) {
            if(autoCommitConf.toLowerCase().equals("false"))
                this.autoCommit = false;
            else if(autoCommitConf.toLowerCase().equals("true"))
                this.autoCommit = true;
        }

        this.groupId = props.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        if(this.groupId == null || this.groupId.length() == 0) {
            throw new IllegalArgumentException(ConsumerConfig.GROUP_ID_CONFIG + " must be specified.");
        }

        this.consumer = new KafkaConsumer(props);
    }

    public void close() {
        if(consumer != null)
            consumer.close();
    }

    public void register4Wakeup() {
        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Starting exit...");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
