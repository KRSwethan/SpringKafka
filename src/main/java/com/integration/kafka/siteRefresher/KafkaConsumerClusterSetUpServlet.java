package com.integration.kafka.siteRefresher;

import com.integration.kafka.config.KafkaIntegrationSpringContext;
import com.integration.kafka.config.KafkaSiteConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class KafkaConsumerClusterSetUpServlet extends HttpServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerClusterSetUpServlet.class);

    public void doGet(HttpServletRequest request,
                      HttpServletResponse response) throws IOException {
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        try {
            String activateConsumer = request.getParameter("activateConsumerCluster");
            LOGGER.info("activateConsumer : " + activateConsumer);
            KafkaSiteConfigs kafkaSiteConfigs = KafkaIntegrationSpringContext.getContext().getBean(KafkaSiteConfigs.class);
            kafkaSiteConfigs.setUpKafkaConsumerCluster(activateConsumer);
            LOGGER.debug("SetUp of Kafka Consumers Beans Executed");
            out.println("<h1>SetUp of Kafka Consumers Beans Executed.</h1>");
        } catch (Exception e) {
            LOGGER.error("context", e);
            out.println("<h1>Error in SetUp of Kafka Consumers Beans</h1>");
        } finally {
            if (null != out) {
                out.flush();
                out.close();
            }
            LOGGER.debug("exiting doGet");
        }
    }
}
