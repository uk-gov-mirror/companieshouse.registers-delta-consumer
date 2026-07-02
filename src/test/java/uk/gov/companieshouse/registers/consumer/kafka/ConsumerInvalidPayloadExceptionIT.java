package uk.gov.companieshouse.registers.consumer.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.companieshouse.registers.consumer.kafka.KafkaUtils.ERROR_TOPIC;
import static uk.gov.companieshouse.registers.consumer.kafka.KafkaUtils.INVALID_TOPIC;
import static uk.gov.companieshouse.registers.consumer.kafka.KafkaUtils.MAIN_TOPIC;
import static uk.gov.companieshouse.registers.consumer.kafka.KafkaUtils.RETRY_TOPIC;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import uk.gov.companieshouse.registers.consumer.service.DeltaServiceRouter;

@SpringBootTest
class ConsumerInvalidPayloadExceptionIT extends AbstractKafkaIT {

    @Autowired
    private KafkaConsumer<String, byte[]> testConsumer;

    @Autowired
    private KafkaProducer<String, byte[]> testProducer;

    @MockitoBean
    private DeltaServiceRouter deltaServiceRouter;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("steps", () -> 1);
    }

    @BeforeEach
    public void drainKafkaTopics() {
        testConsumer.poll(Duration.ofMillis(1000));
    }

    @Test
    void testPublishToRegistersInvalidMessageTopicIfInvalidDataDeserialised() throws Exception {
        // given
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
        DatumWriter<String> writer = new ReflectDatumWriter<>(String.class);
        writer.write("bad data", encoder);

        // when
        testProducer.send(new ProducerRecord<>(MAIN_TOPIC, 0, System.currentTimeMillis(),
                "key", outputStream.toByteArray()));

        // then
        ConsumerRecords<?, ?> consumerRecords = KafkaTestUtils.getRecords(testConsumer, Duration.ofMillis(10000L), 2);
        assertThat(KafkaUtils.noOfRecordsForTopic(consumerRecords, MAIN_TOPIC)).isOne();
        assertThat(KafkaUtils.noOfRecordsForTopic(consumerRecords, RETRY_TOPIC)).isZero();
        assertThat(KafkaUtils.noOfRecordsForTopic(consumerRecords, ERROR_TOPIC)).isZero();
        assertThat(KafkaUtils.noOfRecordsForTopic(consumerRecords, INVALID_TOPIC)).isOne();
    }
}
