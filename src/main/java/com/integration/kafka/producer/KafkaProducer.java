package com.integration.kafka.producer;

import com.integration.kafka.config.KafkaIntegrationSpringContext;
import com.integration.kafka.config.KafkaGeneralProperties;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;

@Component
public class KafkaProducer implements IKafkaProducer {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

    @Autowired
    private ProduceToTopic produceToTopic;

    @Override
    public SendResult<String, String> produceToTopic(String topic, String messageKey, String message, String headerName, String headerValue) throws Exception {
        if(null != produceToTopic) {
            return produceToTopic.produce(topic, messageKey, message, 0, headerName, headerValue);
        } else {
            ProduceToTopic produceToTopic = KafkaIntegrationSpringContext.getBean(ProduceToTopic.class);
            return produceToTopic.produce(topic, messageKey, message, 0, headerName, headerValue);
        }
    }

    @Override
    public SendResult<String, String> produceToTopic(String topic, String messageKey, String message) throws Exception {
        if(null != produceToTopic) {
            return produceToTopic.produce(topic, messageKey, message, 0, null, null);
        } else {
            ProduceToTopic produceToTopic = KafkaIntegrationSpringContext.getBean(ProduceToTopic.class);
            return produceToTopic.produce(topic, messageKey, message, 0, null, null);
        }
    }

    @Service
    class ProduceToTopic {

        private final Logger LOGGER = LoggerFactory.getLogger(ProduceToTopic.class);

        @Autowired
        private KafkaGeneralProperties kafkaGeneralProperties;

        public SendResult<String, String> produce(String topic, String messageKey, String message, int sendCount,
                                                  String headerName, String headerValue) throws Exception {
            ListenableFuture<SendResult<String, String>> sendResultListenableFuture;
            SendResult<String, String> sendResult = null;
            try {
                Message<String> messageObj = null;
                if(StringUtils.isNotEmpty(headerName)) {
                    messageObj = MessageBuilder
                            .withPayload(message)
                            .setHeader(KafkaHeaders.TOPIC, topic)
                            .setHeader(KafkaHeaders.MESSAGE_KEY, messageKey)
                            .setHeader(headerName, headerValue)
                            .build();
                } else {
                    messageObj = MessageBuilder
                            .withPayload(message)
                            .setHeader(KafkaHeaders.TOPIC, topic)
                            .setHeader(KafkaHeaders.MESSAGE_KEY, messageKey)
                            .build();
                }
                KafkaTemplate kafkaTemplate = (KafkaTemplate) KafkaIntegrationSpringContext.getContext().getBean("kafkaTemplate");
                LOGGER.info("Produce to topic: " + topic);
                sendResultListenableFuture = kafkaTemplate.send(messageObj);
                sendResult = sendResultListenableFuture.get();
            } catch (ConfigException e) {
                LOGGER.info("ConfigException: produce is not Successful: " + e.getMessage());
                throw e;
            } catch (ExecutionException | InterruptedException e) {
                LOGGER.info("ExecutionException | InterruptedException: produce is not Successful: " + e.getMessage());
                if (sendCount < kafkaGeneralProperties.getRetryCount()) {
                    produce(topic, messageKey, message, sendCount+1, headerName, headerValue);
                } else {
                    LOGGER.info(e.getMessage());
                    throw e;
                }
            } catch (Exception e) {
                LOGGER.info("Exception: produce is not Successful: " + e.getMessage());
                if (sendCount < kafkaGeneralProperties.getRetryCount()) {
                    produce(topic, messageKey, message, sendCount+1, headerName, headerValue);
                } else {
                    LOGGER.info(e.getMessage());
                    throw e;
                }
            }
            return sendResult;
        }
    }
}
