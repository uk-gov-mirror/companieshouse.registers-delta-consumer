package uk.gov.companieshouse.registers.consumer.serdes;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import tools.jackson.databind.json.JsonMapper;

@Configuration
public class SerdesConfig {

    @Primary
    @Bean
    public JsonMapper objectMapper() {
        return JsonMapper.builder()
                // No module registration needed; java.time support is native in Jackson 3!
                .changeDefaultPropertyInclusion(inclusion -> inclusion.withValueInclusion(JsonInclude.Include.NON_NULL))
                .build();
    }
}
