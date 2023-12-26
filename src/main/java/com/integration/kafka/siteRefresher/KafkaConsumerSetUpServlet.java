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

public class KafkaConsumerSetUpServlet extends HttpServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerSetUpServlet.class);

    public void doGet(HttpServletRequest request,
                      HttpServletResponse response) throws IOException {
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        try {
            String listenerId = request.getParameter("listenerId");
            String enableConsumer = request.getParameter("enableConsumer");
            LOGGER.info("enableConsumer : " + enableConsumer);
            LOGGER.info("listenerId : " + listenerId);
            KafkaSiteConfigs kafkaSiteConfigs = KafkaIntegrationSpringContext.getContext().getBean(KafkaSiteConfigs.class);
            kafkaSiteConfigs.setUpKafkaConsumerBean(enableConsumer, listenerId);
            LOGGER.debug("SetUp of Kafka Consumer Bean Executed");
            out.println("<h1>SetUp of Kafka Consumer Bean Executed.</h1>");
        } catch (Exception e) {
            LOGGER.error("context", e);
            out.println("<h1>Error in SetUp of Kafka Consumer Bean</h1>");
        } finally {
            if (null != out) {
                out.flush();
                out.close();
            }
            LOGGER.debug("exiting doGet");
        }
    }
}
