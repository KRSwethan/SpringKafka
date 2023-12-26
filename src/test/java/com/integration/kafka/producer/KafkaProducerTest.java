package com.integration.kafka.producer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "/test-application-context.xml")
public class KafkaProducerTest {

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private KafkaProducer.ProduceToTopic produceToTopic;

    @Test
    public void testProducer(){
        try {
            //Thread.sleep(10000000);
            for(int i = 0; i < 1; i++) {
                for(int j = 0; j < 1; j++) {
                    String message = new StringBuilder("ABC").toString();

                    //String xml = "testJob";
                    SendResult<String, String> sendResult = kafkaProducer.
                            produceToTopic("testTopic", null, message);
                    System.out.println("topic: " + sendResult.getProducerRecord().topic());
                    System.out.println("value of i : " + i);
                }
            }

        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        }
    }
}
