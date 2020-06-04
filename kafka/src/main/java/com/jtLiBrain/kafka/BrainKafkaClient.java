package com.jtLiBrain.kafka;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Properties;

public class BrainKafkaClient {
    private AdminClient adminClient;

    public void start(Properties props) {
        adminClient = AdminClient.create(props);
    }

    public void stop() {
        if(adminClient != null) {
            adminClient.close();
        }
    }
}
