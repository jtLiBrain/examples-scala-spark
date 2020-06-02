package com.jtLiBrain.examples.flume;

import org.apache.flume.node.Application;

public class EngineLauncher {
    private ConfigurationProvider confProvider;

    public void start() {
        Application application = new Application();
        application.handleConfigurationEvent(confProvider.getConfiguration());
        application.start();
    }

    public ConfigurationProvider getConfProvider() {
        return confProvider;
    }
    public void setConfProvider(ConfigurationProvider confProvider) {
        this.confProvider = confProvider;
    }
}
