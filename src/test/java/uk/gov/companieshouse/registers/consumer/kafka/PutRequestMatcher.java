package uk.gov.companieshouse.registers.consumer.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.matching.MatchResult;
import com.github.tomakehurst.wiremock.matching.ValueMatcher;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;
import uk.gov.companieshouse.api.registers.InternalRegisters;

public class PutRequestMatcher implements ValueMatcher<Request> {

    private static final JsonMapper MAPPER = JsonMapper.builder()
            // No module registration needed; java.time support is native in Jackson 3!
            .changeDefaultPropertyInclusion(inclusion -> inclusion.withValueInclusion(JsonInclude.Include.NON_NULL))
            .build();

    private final String expectedUrl;
    private final String expectedBody;

    public PutRequestMatcher(String expectedUrl, String expectedBody) {
        this.expectedUrl = expectedUrl;
        this.expectedBody = expectedBody;
    }

    @Override
    public MatchResult match(Request value) {
        return MatchResult.aggregate(
                matchUrl(value.getUrl()),
                matchMethod(value.getMethod()),
                matchBody(value.getBodyAsString()));
    }

    private MatchResult matchUrl(String actualUrl) {
        return MatchResult.of(expectedUrl.equals(actualUrl));
    }

    private MatchResult matchMethod(RequestMethod actualMethod) {
        return MatchResult.of(RequestMethod.PUT.equals(actualMethod));
    }

    private MatchResult matchBody(String actualBody) {

        try {
            InternalRegisters expected = MAPPER.readValue(expectedBody, InternalRegisters.class);
            InternalRegisters actual = MAPPER.readValue(actualBody, InternalRegisters.class);

            MatchResult result = MatchResult.of(expected.equals(actual));
            if (!result.isExactMatch()) {
                System.out.printf("%nExpected: [%s]%n", expected);
                System.out.printf("%nActual: [%s]", actual);
            }
            return result;
        } catch (JacksonException ex) {
            return MatchResult.of(false);
        }
    }
}

