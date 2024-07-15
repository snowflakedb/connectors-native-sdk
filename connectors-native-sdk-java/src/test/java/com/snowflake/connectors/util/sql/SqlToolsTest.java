package com.snowflake.connectors.util.sql;

import static com.snowflake.connectors.util.sql.SqlTools.asCommaSeparatedSqlList;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static com.snowflake.connectors.util.sql.SqlTools.asVariant;
import static com.snowflake.connectors.util.sql.SqlTools.quoted;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.snowpark_java.types.Variant;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class SqlToolsTest {

  @Test
  void shouldFormatStringAsVarchar() {
    // expect
    assertThat(asVarchar("abc")).isEqualTo("'abc'");
    assertThat(asVarchar("a'b'c")).isEqualTo("'a\\'b\\'c'");
    assertThat(asVarchar(null)).isEqualTo(null);
  }

  @Test
  void shouldEscapeVariantAndWrapInParseJson() {
    // given
    var variant1 = new Variant("{\"abc\":\"def\"}");
    var variant2 = new Variant("{\"abc\":\"d'e'f\"}");
    var variant3 = new Variant("{\"abc\":\"d\\\"e\\\"f\"}");

    // expect
    assertThat(asVariant(variant1)).isEqualTo("PARSE_JSON('{\"abc\":\"def\"}')");
    assertThat(asVariant(variant2)).isEqualTo("PARSE_JSON('{\"abc\":\"d\\'e\\'f\"}')");
    assertThat(asVariant(variant3)).isEqualTo("PARSE_JSON('{\"abc\":\"d\\\\\\\"e\\\\\\\"f\"}')");
    assertThat(asVariant(null)).isEqualTo(null);
  }

  @Test
  void shouldFormatStringsAsVarcharsAndJoin() {
    // given
    var strings = Arrays.asList("abc", "d'e'f", null);

    // expect
    assertThat(asCommaSeparatedSqlList(strings)).isEqualTo("'abc','d\\'e\\'f',null");
  }

  @Test
  void shouldQuoteString() {
    // expect
    assertThat(quoted("abc")).isEqualTo("\"abc\"");
    assertThat(quoted("a\"b\"c")).isEqualTo("\"a\"b\"c\"");
    assertThat(quoted(null)).isEqualTo(null);
  }
}
