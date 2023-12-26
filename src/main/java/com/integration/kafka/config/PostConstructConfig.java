package com.integration.kafka.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class PostConstructConfig implements ApplicationListener<ContextRefreshedEvent> {

    @Autowired
    @Qualifier("kafkaSiteConfig")
    private KafkaSiteConfigs kafkaSiteConfigs;

    public PostConstructConfig() {
    }

    public void setKafkaSiteConfigs(KafkaSiteConfigs kafkaSiteConfigs) {
        this.kafkaSiteConfigs = kafkaSiteConfigs;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        Map<String, String> toDisableConsumerBeans = KafkaIntergrationConstants.disableConsumerMap;
        toDisableConsumerBeans.forEach((k, v) -> kafkaSiteConfigs.setUpKafkaConsumerBean(KafkaIntergrationConstants.str_false, v));
    }
}
