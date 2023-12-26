package com.integration.kafka.config;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaIntergrationConstants {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIntergrationConstants.class);

    public static String bootStrapServers;
    public static boolean isActiveConsumerCluster;
    public static String externalPropertyFileLocation;
    public static Integer consumerConcurrency;
    public static Integer consumerMaxPollTimeOut;
    public static Integer consumerSessionTimeOut;
    public static Integer consumerMaxPollRecords;
    public static Map<String, String> listenerTopicMap;
    public static Map<String, String> listenerGroupIdMap;
    public static Map<String, String> disableConsumerMap = new HashMap<>();

    //active-active helpers

    public static final String str_true = "true";
    public static final String str_false = "false";

    protected static Properties externalProperties;
    protected static Properties internalProperties;

    static {
        try {
            LOGGER.info("Kafka integration version 1.3.2");
            LOGGER.info("Loading Internal/External Properties");
            String filename = "kafkaIntegrationConfigs.properties";
            InputStream input = KafkaIntergrationConstants.class.getClassLoader().getResourceAsStream(filename);
            internalProperties = readPropertyFile(input);
            externalPropertyFileLocation = internalProperties.getProperty("kafka.externalPropFile.Location");
            externalProperties = readPropertyFile(externalPropertyFileLocation);
            loadExteralProps();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            internalProperties = null;
        }
    }

    public static Properties readPropertyFile(InputStream input) throws IOException {
        Properties properties = new Properties();
        properties.load(input);
        return properties;
    }

    public static void loadExteralProps() {
        bootStrapServers = externalProperties.getProperty("integration.kafka.bootStrapServers");
        isActiveConsumerCluster = "true".equalsIgnoreCase(externalProperties.getProperty("integration.kafka.isActiveConsumerCluster"));
        consumerConcurrency = Integer.valueOf(externalProperties.getProperty("integration.kafka.consumer.concurrency"));
        consumerMaxPollTimeOut = Integer.valueOf(externalProperties.getProperty("integration.kafka.consumer.max.poll.interval.ms"));
        consumerSessionTimeOut = Integer.valueOf(externalProperties.getProperty("integration.kafka.consumer.session.timeout.ms"));
        String[] consumerTopicArr = externalProperties.getProperty("listener.topic.Map").split(",");
        listenerTopicMap = getMapFromArray(consumerTopicArr);
        String[] consumerGroupIdArr = externalProperties.getProperty("listener.groupId.Map").split(",");
        listenerGroupIdMap = getMapFromArray(consumerGroupIdArr);
        String disableConsumers = externalProperties.getProperty("disable.listener.map");
        if(StringUtils.isNotEmpty(disableConsumers)) {
            String[] disableConsumerArr = disableConsumers.split(",");
            disableConsumerMap = getMapFromArray(disableConsumerArr);
        }
    }

    public static Properties readPropertyFile(String fileLocation) throws IOException {
        File file = new File(fileLocation);
        InputStream input = new FileInputStream(file);
        Properties properties = new Properties();
        properties.load(input);
        return properties;
    }

    private static Map<String, String> getMapFromArray(String[] strArr) {
        return Arrays.asList(strArr).stream().map(elem -> elem.split(":")).collect(Collectors.toMap(e -> e[0], e -> e[1]));
    }
}
