package com.jtLiBrain.examples.flume;

import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.node.AbstractConfigurationProvider;

import java.util.Map;

public class ConfigurationProvider extends AbstractConfigurationProvider {
    private Map<String, String> properties;

    public ConfigurationProvider(String agentName) {
        super(agentName);
    }

    @Override
    protected FlumeConfiguration getFlumeConfiguration() {
        FlumeConfiguration flumeConf = new FlumeConfiguration(properties);
        return flumeConf;
    }
}
