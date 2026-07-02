package uk.gov.companieshouse.registers.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import uk.gov.companieshouse.registers.consumer.kafka.AbstractKafkaIT;

import java.util.concurrent.TimeUnit;

@AutoConfigureTestRestTemplate
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RegistersApplicationIT extends AbstractKafkaIT {

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Test
    void shouldStartApplication() {
        Executable executable = () -> Application.main(new String[0]);
        assertDoesNotThrow(executable);
    }

    @Test
    void shouldReturn200FromGetHealthEndpoint() {

        await()
                .atMost(5, TimeUnit.MINUTES)
                .pollInterval(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    ResponseEntity<String> response = testRestTemplate.getForEntity("/healthcheck", String.class);

                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                    assertThat(response.getBody()).contains("\"status\":\"UP\"");
                });


    }
}
