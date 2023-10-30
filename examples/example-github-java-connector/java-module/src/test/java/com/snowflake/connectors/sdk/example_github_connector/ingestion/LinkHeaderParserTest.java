package com.snowflake.connectors.sdk.example_github_connector.ingestion;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class LinkHeaderParserTest {

    @Test
    void testParsingLinkHeader() {
        // given
        String linkHeader = "<https://api.github.com/repositories/33884891/issues?page=27>; rel=\"prev\", <https://api.github.com/repositories/33884891/issues?page=1>; rel=\"first\"";

        // when
        var result = new LinkHeaderParser().parseLink(linkHeader);

        // then
        Map<String, String> expected = Map.of(
                "prev", "https://api.github.com/repositories/33884891/issues?page=27",
                "first", "https://api.github.com/repositories/33884891/issues?page=1"
        );
        assertTrue(result.equals(expected));
    }
}
