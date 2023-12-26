package com.integration.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "/test-application-context.xml")
public class KafkaConsumerTest {

    public static final String MY_TOPIC_1 = "testTopic1";
    @Autowired
    @Qualifier("conKafkaMessageListenerContnrFctry")
    public ConcurrentKafkaListenerContainerFactory concurrentKafkaListenerContainerFactory;
	
    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
	
    String message = "ABC";
	
    @Autowired
    private Listener listener;
    @Autowired
    @Qualifier("kafkaTemplate")
    private KafkaTemplate kafkaTemplate;

    @Test
    public void testSendConsumeMessage() throws Exception {
        Message<String> messageObj = MessageBuilder
                .withPayload(message)
                .setHeader(KafkaHeaders.TOPIC, MY_TOPIC_1)
                .build();
        //kafkaTemplate.send(messageObj);
        kafkaTemplate.send(MY_TOPIC_1, 1, null, message);
        kafkaTemplate.flush();
        kafkaListenerEndpointRegistry.start();
        Assert.assertTrue(this.listener.latch.await(60, TimeUnit.SECONDS));
    }


    @Configuration
    @EnableKafka
    public static class Config {
        @Bean
        public KafkaEmbedded kafkaEmbedded() {
            return new KafkaEmbedded(1, true, MY_TOPIC_1);
        }
    }
}

@Component
class Listener {
    public final CountDownLatch latch = new CountDownLatch(1);

    @KafkaListener(id = "listener1", groupId = "group02",
            topics = {"testTopic1"}, containerFactory = "conKafkaMessageListenerContnrFctry")
    public void onMessage(ConsumerRecord<String, String> consumerRecord, @Headers MessageHeaders messageHeaders) {
        try {
            this.latch.countDown();
        } catch (Throwable throwable) {
            System.out.println("Exception from Consumer.onMessage : " + throwable.getMessage());
        }
    }
}
