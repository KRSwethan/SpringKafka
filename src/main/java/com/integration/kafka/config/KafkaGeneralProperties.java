package com.integration.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component("kafkaGeneralProperties")
public class KafkaGeneralProperties {

    //Common properties
    //bootStrapServers/siteHlthChckrBootStrapServers/topicsArray/siteHealthCheckerTopic properties
    private String bootStrapServers = KafkaIntergrationConstants.bootStrapServers;

    //Producer properties
    @Value("${integration.kafka.acks}")
    private String acks;

    @Value("${integration.kafka.retries}")
    private String retries;

    @Value("${integration.kafka.enable.idempotence}")
    private Boolean enableIdempotence;

    @Value("${integration.kafka.transactional.id}")
    private String transactionId;

    @Value("${integration.kafka.produce.retryCount}")
    private Integer retryCount;

    //Consumer properties
    private int concurrency = KafkaIntergrationConstants.consumerConcurrency;

    private int consumerMaxPollTimeOut = KafkaIntergrationConstants.consumerMaxPollTimeOut;

    private int consumerSessionTimeOut = KafkaIntergrationConstants.consumerSessionTimeOut;

    @Value("${integration.kafka.enable.auto.commit}")
    private Boolean enableAutoCommit;

    @Value("${integration.kafka.isolation.level}")
    private String isolationLevel;

    //Transactional Producer properties
    @Value("${integration.kafka.acks.all}")
    private String acksAll;

    @Value("${integration.kafka.enable.acks.all.Idempotence}")
    private Boolean acksAllIdempotence;

    @Value("${integration.kafka.acks.all.retries}")
    private Integer acksAllRetries;

    //listener properties
    private Map<String, String> listenerTopicMap = KafkaIntergrationConstants.listenerTopicMap;

    private Map<String, String> listenerGroupIdMap = KafkaIntergrationConstants.listenerGroupIdMap;

    public String getBootStrapServers() {
        return bootStrapServers;
    }

    public String getAcks() {
        return acks;
    }

    public String getRetries() {
        return retries;
    }

    public Boolean getEnableIdempotence() {
        return enableIdempotence;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public Integer getRetryCount() {
        return retryCount;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public int getConsumerMaxPollTimeOut() {
        return consumerMaxPollTimeOut;
    }

    public int getConsumerSessionTimeOut() {
        return consumerSessionTimeOut;
    }

    public Boolean getEnableAutoCommit() {
        return enableAutoCommit;
    }

    public String getIsolationLevel() {
        return isolationLevel;
    }

    public String getAcksAll() {
        return acksAll;
    }

    public Boolean getAcksAllIdempotence() {
        return acksAllIdempotence;
    }

    public Integer getAcksAllRetries() {
        return acksAllRetries;
    }

    public Map<String, String> getListenerTopicMap() {
        return listenerTopicMap;
    }

    public Map<String, String> getListenerGroupIdMap() {
        return listenerGroupIdMap;
    }
}
