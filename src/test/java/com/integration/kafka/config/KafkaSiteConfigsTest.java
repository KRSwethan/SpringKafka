package com.integration.kafka.config;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.config.AbstractKafkaListenerContainerFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "/test-application-context.xml")
public class KafkaSiteConfigsTest {

    @Autowired
    private KafkaSiteConfigs kafkaSiteConfigs;

    @Autowired
    @Qualifier("conKafkaMessageListenerContnrFctry")
    private ConcurrentKafkaListenerContainerFactory concurrentKafkaListenerContainerFactory;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Test
    public void testStart() {
        AbstractKafkaListenerContainerFactory abstractKafkaListenerContainerFactory = concurrentKafkaListenerContainerFactory;
        kafkaSiteConfigs.setUpKafkaConsumerCluster("true");
    }
}
