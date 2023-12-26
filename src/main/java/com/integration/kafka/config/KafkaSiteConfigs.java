package com.integration.kafka.config;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.NonResponsiveConsumerEvent;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.*;

@Component("kafkaSiteConfig")
public class KafkaSiteConfigs {

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSiteConfigs.class);

    private String bootStrapServers = KafkaIntergrationConstants.bootStrapServers;

    public String getBootStrapServers() {
       return bootStrapServers;
    }

    @EventListener()
    public void switchSitesOnNonResponseError(NonResponsiveConsumerEvent event) {
        //When Kafka server is down, NonResponsiveConsumerEvent error is caught here.
        LOGGER.info("NonResponsiveConsumerEvent from KafkaBrokers caught for instance " + event.getSource());
    }

    public void setUpKafkaConsumerCluster(String activateConsumerCluster) {
        try {
            if(StringUtils.isNotEmpty(activateConsumerCluster)) {
                LOGGER.info("isActiveConsumerCluster : " + activateConsumerCluster);
                if(KafkaIntergrationConstants.str_false.equalsIgnoreCase(activateConsumerCluster)) {
                    KafkaIntergrationConstants.isActiveConsumerCluster = false;
                    stopConsumerCluster();
                } else if(KafkaIntergrationConstants.str_true.equalsIgnoreCase(activateConsumerCluster)) {
                    KafkaIntergrationConstants.isActiveConsumerCluster = true;
                    startConsumerCluster();
                }
                LOGGER.info("KafkaIntergrationConstants.isActiveConsumerCluster : " + KafkaIntergrationConstants.isActiveConsumerCluster);
            }
            LOGGER.info("SetUp of Kafka Consumers Beans Successful");
        } catch (Exception e) {
            LOGGER.info("Exception from KafkaSiteConfigs.setUpKafkaConsumerCluster: " + e.getMessage());
        }
    }

    private void stopConsumerCluster() {
        Collection<MessageListenerContainer> kafkaListenerMap;
        try {
            LOGGER.info("stop Consumer Beans initiated");
            kafkaListenerMap = kafkaListenerEndpointRegistry.getListenerContainers();
            Iterator iterator = kafkaListenerMap.iterator();
            while(iterator.hasNext()) {
                ConcurrentMessageListenerContainer messageListenerContainer = (ConcurrentMessageListenerContainer)iterator.next();
                if(messageListenerContainer.isRunning()) {
                    messageListenerContainer.stop();
                    messageListenerContainer.setAutoStartup(false);
                    LOGGER.info(new StringBuilder("Listener ").append(messageListenerContainer.getBeanName()).append(" stopped").toString());
                }
            }
        } catch (Exception e) {
            LOGGER.info("Exception from stopConsumerBeans : " + e.getCause());
            e.printStackTrace();
            throw e;
        }
    }

    private void startConsumerCluster() {
        Collection<MessageListenerContainer> kafkaListenerMap;
        try {
            LOGGER.info("start Consumer Beans initiated");
            kafkaListenerMap = kafkaListenerEndpointRegistry.getListenerContainers();
            Iterator iterator = kafkaListenerMap.iterator();
            while(iterator.hasNext()) {
                ConcurrentMessageListenerContainer messageListenerContainer = (ConcurrentMessageListenerContainer)iterator.next();
                messageListenerContainer.setAutoStartup(true);
                messageListenerContainer.start();
                LOGGER.info(new StringBuilder("Listener ").append(messageListenerContainer.getBeanName()).append(" started").toString());
            }
            KafkaIntergrationConstants.disableConsumerMap.forEach((k, v) -> setUpKafkaConsumerBean(KafkaIntergrationConstants.str_false, v));
        } catch (Exception e) {
            LOGGER.info("Exception from startConsumerCluster : " + e.getCause());
            e.printStackTrace();
            throw e;
        }
    }

    public void setUpKafkaConsumerBean(String enableConsumer, String consumerName) {
        try {
            if(StringUtils.isNotEmpty(enableConsumer)) {
                LOGGER.info(new StringBuilder("listenerId : ").append(consumerName).append(", enableConsumer : ").append(enableConsumer).toString());
                if(KafkaIntergrationConstants.str_true.equalsIgnoreCase(enableConsumer)) {
                    startSpecificConsumerBean(consumerName);
                } else if (KafkaIntergrationConstants.str_false.equalsIgnoreCase(enableConsumer)){
                    stopSpecificConsumerBean(consumerName);
                }
            }
        } catch (Exception e) {
            LOGGER.info("Exception from KafkaSiteConfigs.setUpKafkaConsumerBean: " + e.getMessage());
        }
    }

    private void startSpecificConsumerBean(String consumerBeanName) {
        Collection<MessageListenerContainer> kafkaListenerMap;
        try {
            LOGGER.info(new StringBuilder("start consumer initiated for listenerId : ").append(consumerBeanName).toString());
            kafkaListenerMap = kafkaListenerEndpointRegistry.getListenerContainers();
            Iterator iterator = kafkaListenerMap.iterator();
            while(iterator.hasNext()) {
                ConcurrentMessageListenerContainer messageListenerContainer = (ConcurrentMessageListenerContainer) iterator.next();
                if(messageListenerContainer.getBeanName().equalsIgnoreCase(consumerBeanName) &&
                        !messageListenerContainer.isRunning()) {
                    messageListenerContainer.setAutoStartup(true);
                    messageListenerContainer.start();
                    LOGGER.info(new StringBuilder("Listener ").append(messageListenerContainer.getBeanName()).append(" started").toString());
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.info("Exception from startSpecificConsumerBeans : " + e.getCause());
            e.printStackTrace();
            throw e;
        }
    }

    private void stopSpecificConsumerBean(String consumerBeanName) {
        Collection<MessageListenerContainer> kafkaListenerMap;
        try {
            LOGGER.info(new StringBuilder("stop consumer initiated for listenerId : ").append(consumerBeanName).toString());
            kafkaListenerMap = kafkaListenerEndpointRegistry.getListenerContainers();
            Iterator iterator = kafkaListenerMap.iterator();
            while(iterator.hasNext()) {
                ConcurrentMessageListenerContainer messageListenerContainer = (ConcurrentMessageListenerContainer)iterator.next();
                if(messageListenerContainer.getBeanName().equalsIgnoreCase(consumerBeanName) &&
                        messageListenerContainer.isRunning()) {
                    messageListenerContainer.stop();
                    messageListenerContainer.setAutoStartup(false);
                    LOGGER.info(new StringBuilder("Listener ").append(messageListenerContainer.getBeanName()).append(" stopped").toString());
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.info("Exception from stopConsumerBeans : " + e.getCause());
            e.printStackTrace();
            throw e;
        }
    }
}
