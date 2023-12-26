package com.integration.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaIntegrationConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIntegrationConsumer.class);

    @KafkaListener(id = "listenerOne", groupId = "#{kafkaGeneralProperties.getListenerGroupIdMap().get(\"listenerOne\")}",
            topics = {"#{kafkaGeneralProperties.getListenerTopicMap().get(\"listenerOne\")}"},
            containerFactory = "conKafkaMessageListenerContnrFctry",
            idIsGroup = false)
    public void onKafkaMessageOne(ConsumerRecord<String, String> consumerRecord) {
        try {
            LOGGER.info(new StringBuilder("Message Consumed from Topic : ").append(consumerRecord.topic())
                    .append("from Partition: ").append(consumerRecord.partition()).toString());
        } catch (Throwable throwable) {
            LOGGER.info("Exception from PrimaryKafkaConsumer.onMessage : ", throwable);
        }
    }

    @KafkaListener(id = "listenerTwo",
            groupId = "#{kafkaGeneralProperties.getListenerGroupIdMap().get(\"listenerTwo\")}",
            topics = {"#{kafkaGeneralProperties.getListenerTopicMap().get(\"listenerTwo\")}"},
            containerFactory = "conKafkaMessageListenerContnrFctry",
            idIsGroup = false)
    public void onKafkaMessageTwo(ConsumerRecord<String, String> consumerRecord) {
        try {
            LOGGER.info(new StringBuilder("Message Consumed from Topic : ").append(consumerRecord.topic())
                    .append("from Partition: ").append(consumerRecord.partition()).toString());
        } catch (Throwable throwable) {
            LOGGER.info("Exception from PrimaryKafkaConsumer.onMessage : ", throwable);
        }
    }
}
