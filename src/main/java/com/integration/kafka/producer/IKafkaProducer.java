package com.integration.kafka.producer;

import org.springframework.kafka.support.SendResult;

public interface IKafkaProducer {

    SendResult<String, String> produceToTopic(String topic, String messageKey, String message, String headerName, String headerValue) throws Exception ;

    public SendResult<String, String> produceToTopic(String topic, String messageKey, String message) throws Exception;
}
