package com.integration.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration()
@EnableKafka
public class KafkaSpringConfigs {

    @Autowired
    @Qualifier("kafkaGeneralProperties")
    private KafkaGeneralProperties kafkaGeneralProperties;

    @Autowired
    @Qualifier("kafkaSiteConfig")
    private KafkaSiteConfigs kafkaSiteConfigs;

    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Bean("kafkaTemplate")
    public KafkaTemplate kafkaTemplate() {
        return kafkaTemplateWrapper();
    }

    public KafkaTemplate kafkaTemplateWrapper() {
        KafkaTemplate kafkaTemplate = new KafkaTemplate(defaultKafkaProducerFactory());
        return kafkaTemplate;
    }

    public ProducerFactory defaultKafkaProducerFactory() {
        ProducerFactory defaultKafkaProducerFactory = new DefaultKafkaProducerFactory(producerProps());
        return defaultKafkaProducerFactory;
    }

    private Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaIntergrationConstants.bootStrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, kafkaGeneralProperties.getAcks());
        props.put(ProducerConfig.RETRIES_CONFIG, kafkaGeneralProperties.getRetries());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, kafkaGeneralProperties.getEnableIdempotence());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        return props;
    }

    @Bean("conKafkaMessageListenerContnrFctry")
    public ConcurrentKafkaListenerContainerFactory<String, String> conKafkaMessageListenerContnrFctry() {
        ConcurrentKafkaListenerContainerFactory<String, String> conKafkaListenerContainerFactory = conKafkaListenerContainerFctryWrpr();
        if(!KafkaIntergrationConstants.isActiveConsumerCluster) {
            conKafkaListenerContainerFactory.setAutoStartup(false);
        }
        return conKafkaListenerContainerFactory;
    }

    public ConcurrentKafkaListenerContainerFactory<String, String> conKafkaListenerContainerFctryWrpr() {
        ConcurrentKafkaListenerContainerFactory kafkaMessageListenerContainer = new ConcurrentKafkaListenerContainerFactory();
        kafkaMessageListenerContainer.setConsumerFactory(defaultKafkaConsumerFactoryWrapper());
        kafkaMessageListenerContainer.setConcurrency(kafkaGeneralProperties.getConcurrency());
        return kafkaMessageListenerContainer;
    }

    public ConsumerFactory defaultKafkaConsumerFactoryWrapper() {
        ConsumerFactory consumerFactory = new DefaultKafkaConsumerFactory(consumerProps());
        return consumerFactory;
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaIntergrationConstants.bootStrapServers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaGeneralProperties.getEnableAutoCommit());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaGeneralProperties.getConsumerSessionTimeOut());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaGeneralProperties.getConsumerMaxPollTimeOut());
        return props;
    }
}
