package uk.gov.companieshouse.registers.consumer.serdes;

import static uk.gov.companieshouse.registers.consumer.Application.NAMESPACE;

import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;
import uk.gov.companieshouse.api.delta.RegisterDelta;
import uk.gov.companieshouse.api.delta.RegistersDeleteDelta;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.registers.consumer.exception.NonRetryableException;
import uk.gov.companieshouse.registers.consumer.logging.DataMapHolder;

@Component
public class RegistersDeltaDeserialiser {

    private static final Logger LOGGER = LoggerFactory.getLogger(NAMESPACE);
    private final JsonMapper objectMapper;

    RegistersDeltaDeserialiser(JsonMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public RegisterDelta deserialiseRegistersDelta(String data) {
        try {
            return objectMapper.readValue(data, RegisterDelta.class);
        } catch (JacksonException ex) {
            LOGGER.error("Unable to deserialise delta: [%s]".formatted(data), ex, DataMapHolder.getLogMap());
            throw new NonRetryableException("Unable to deserialise delta", ex);
        }
    }

    public RegistersDeleteDelta deserialiseRegistersDeleteDelta(String data) {
        try {
            return objectMapper.readValue(data, RegistersDeleteDelta.class);
        } catch (JacksonException ex) {
            LOGGER.error("Unable to deserialise DELETE delta: [%s]".formatted(data), ex, DataMapHolder.getLogMap());
            throw new NonRetryableException("Unable to deserialise DELETE delta", ex);
        }
    }
}
