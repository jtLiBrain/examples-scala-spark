package com.jtLiBrain.examples.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.After;
import org.junit.Before;

import java.util.Properties;

public class AdminClientExample {
    private AdminClient adminClient;

    @Before
    public void before() {
        Properties props = new Properties();

        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        adminClient = AdminClient.create(props);
    }

    @After
    public void after() {
        adminClient.close();
    }


}
