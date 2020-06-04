package com.jtLiBrain.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.After;
import org.junit.Before;

import java.util.Properties;

public class BrainKafkaClientSuite {
    private BrainKafkaClient brainKafkaClient;

    @Before
    public void before() {
        Properties props = new Properties();

        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        brainKafkaClient = new BrainKafkaClient();
        brainKafkaClient.start(props);
    }

    @After
    public void after() {
        brainKafkaClient.stop();
    }
}
