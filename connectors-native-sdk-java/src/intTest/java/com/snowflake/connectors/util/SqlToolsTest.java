/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util;

import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.connectors.BaseIntegrationTest;
import com.snowflake.connectors.common.exception.ConnectorException;
import com.snowflake.connectors.common.response.ConnectorResponse;
import com.snowflake.connectors.util.sql.SqlTools;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SqlToolsTest extends BaseIntegrationTest {

  private static final String TEST_PROCEDURE = "TEST_PROCEDURE";
  private static final String EXAMPLE_NONEXISTENCE_PROCEDURE = "EXAMPLE_NONEXISTENCE_PROCEDURE";

  @BeforeAll
  static void beforeAllTests() {
    createTestProcedure();
  }

  @AfterAll
  static void afterAllTests() {
    session
        .sql("DROP PROCEDURE IF EXISTS PUBLIC." + TEST_PROCEDURE + "(VARCHAR, INTEGER, BOOLEAN)")
        .collect();
  }

  private static void createTestProcedure() {
    String procedureDeclaration =
        "CREATE OR REPLACE PROCEDURE PUBLIC."
            + TEST_PROCEDURE
            + "(arg_a VARCHAR, arg_b INTEGER, invalid_response BOOLEAN) RETURNS VARIANT LANGUAGE"
            + " SQL EXECUTE AS OWNER AS BEGIN IF (invalid_response = TRUE) THEN RETURN"
            + " PARSE_JSON('{\"message\": \"Test only response\"}'); END IF; RETURN"
            + " PARSE_JSON('{\"response_code\": \"OK\", \"message\": \"This is test only OK"
            + " response\"}'); END;";
    session.sql(procedureDeclaration).collect();
  }

  @Test
  void callProcedureMethodShouldCorrectlyCallProcedurePassArgumentAndParseVariantResponse() {
    // given
    String[] procArgs = new String[] {asVarchar("anyTestVarchar"), "1234", "false"};

    // when
    ConnectorResponse response =
        SqlTools.callProcedure(session, "PUBLIC", TEST_PROCEDURE, procArgs);

    // then
    assertThat(response).hasOKResponseCode().hasMessage("This is test only OK response");
  }

  @Test
  void callPublicProcedureMethodShouldCorrectlyCallProcedureCreatedInPUBLICSchema() {
    // given
    String[] procArgs = new String[] {asVarchar("anyTestVarchar"), "1234", "false"};

    // when
    ConnectorResponse response =
        SqlTools.callProcedure(session, "PUBLIC", TEST_PROCEDURE, procArgs);

    // then
    assertThat(response).hasOKResponseCode().hasMessage("This is test only OK response");
  }

  @Test
  void
      callProcedureMethodShouldReturnErrorResponseWhenPassingVarcharArgWithoutToVarcharProcedureArgumentMethodCall() {
    // given
    String[] procArgs = new String[] {"anyTestVarchar", "1234", "false"};

    // expect

    String expectedMessage =
        "Unknown error occurred when calling TEST_PROCEDURE procedure: SQL compilation error: error"
            + " line 1 at position 27\n"
            + "invalid identifier 'ANYTESTVARCHAR'";

    assertThatThrownBy(() -> SqlTools.callPublicProcedure(session, TEST_PROCEDURE, procArgs))
        .isInstanceOf(ConnectorException.class)
        .hasFieldOrPropertyWithValue("response.responseCode", "UNKNOWN_SQL_ERROR")
        .hasFieldOrPropertyWithValue("response.message", expectedMessage)
        .hasMessage(expectedMessage);
  }

  @Test
  void callProcedureMethodShouldReturnErrorResponseWhenCalledProcedureDoesNotExist() {
    // given
    String[] procArgs = new String[] {asVarchar("anyTestVarchar"), "1234", "true"};

    // expect
    String expectedMessage = "Nonexistent procedure " + EXAMPLE_NONEXISTENCE_PROCEDURE + " called";

    assertThatThrownBy(
            () -> SqlTools.callPublicProcedure(session, EXAMPLE_NONEXISTENCE_PROCEDURE, procArgs))
        .isInstanceOf(ConnectorException.class)
        .hasFieldOrPropertyWithValue("response.responseCode", "PROCEDURE_NOT_FOUND")
        .hasFieldOrPropertyWithValue("response.message", expectedMessage)
        .hasMessage(expectedMessage);
  }
}
