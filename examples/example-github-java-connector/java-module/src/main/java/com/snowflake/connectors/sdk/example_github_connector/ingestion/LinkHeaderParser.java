package com.snowflake.connectors.sdk.example_github_connector.ingestion;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

/**
 * Simple parser for Link header.
 * This is NOT a complete implementation of https://www.rfc-editor.org/rfc/rfc5988.html
 */
public class LinkHeaderParser {
    Map<String, String> parseLink(String linkHeader)    {
        return stream(linkHeader.split(",")).map(String::trim).map( it -> {
            var split = it.split(";");
            String url = split[0].substring(1, split[0].length()-1);
            String rawRel = split[1].trim().split("=")[1];
            String rel = rawRel.trim().substring(1, rawRel.length()-1);
            return new AbstractMap.SimpleImmutableEntry<>(rel, url);
        }).collect(Collectors.toMap(AbstractMap.SimpleImmutableEntry::getKey, AbstractMap.SimpleImmutableEntry::getValue));
    }
}
