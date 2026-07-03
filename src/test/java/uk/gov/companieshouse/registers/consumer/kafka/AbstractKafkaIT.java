package uk.gov.companieshouse.registers.consumer.kafka;

import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
@Import(TestKafkaConfig.class)
public abstract class AbstractKafkaIT {

    @Container
    protected static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:4.3.1");

    static {
        kafka.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
}
