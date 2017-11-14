package com.flipkart.rabbitmqspoutexample;

import lombok.extern.slf4j.Slf4j;
import backtype.storm.Config;


@Slf4j
public class Utils {

    public int WINDOW_SIZE = 15;

    public static Config getStormConfiguration() {
        Integer messageTimeoutInSecs = 100;
        Integer numWorkers = 2;
        Integer numAckers = 2;

        Config config = new Config();
        config.setNumAckers(numAckers);
        config.setNumWorkers(numWorkers);
        config.setMessageTimeoutSecs(messageTimeoutInSecs);
        return config;
    }
}
