package com.integration.kafka.config;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "/test-application-context.xml")
public class KafkaIntergrationConstantsTest {

    protected static Properties externalProperties;
    protected static Properties internalProperties;

    @Test
    public void testExternalPropertyLoad() {
        try {
            String filename = "kafkaIntegrationConfigs.properties";
            InputStream input = KafkaIntergrationConstants.class.getClassLoader().getResourceAsStream(filename);
            internalProperties = KafkaIntergrationConstants.readPropertyFile(input);
            String externalPropertyFileLocation = internalProperties.getProperty("kafka.externalPropFile.Location");
            externalProperties = KafkaIntergrationConstants.readPropertyFile(externalPropertyFileLocation);
            Assert.assertTrue(null != externalProperties);
            KafkaIntergrationConstants.loadExteralProps();
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }
}
