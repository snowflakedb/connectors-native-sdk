package com.snowflake.connectors.externalconnection;

import static com.snowflake.connectors.common.SharedObjects.TEST_GH_EAI;
import static com.snowflake.connectors.common.SharedObjects.TEST_SECRET;
import static com.snowflake.connectors.common.assertions.NativeSdkAssertions.assertThat;
import static com.snowflake.connectors.util.sql.SqlTools.arrayConstruct;
import static com.snowflake.connectors.util.sql.SqlTools.asVarchar;

import com.snowflake.connectors.BaseNativeSdkIntegrationTest;
import com.snowflake.connectors.common.SharedObjects;
import com.snowflake.connectors.common.object.ObjectName;
import com.snowflake.connectors.common.procedure.ProcedureDescriptor;
import com.snowflake.connectors.util.sql.SqlTools;
import java.sql.Timestamp;
import java.time.Instant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SetupExternalIntegrationTest extends BaseNativeSdkIntegrationTest {

  private final ProcedureDescriptor procedureDescriptor = new ProcedureDescriptor(session);

  @BeforeAll
  void testInit() {
    executeInApp(
        "GRANT USAGE ON PROCEDURE PUBLIC.SETUP_EXTERNAL_INTEGRATION_WITH_NAMES(ARRAY) TO"
            + " APPLICATION ROLE ADMIN");
    executeInApp(
        "GRANT USAGE ON PROCEDURE PUBLIC.SETUP_EXTERNAL_INTEGRATION_WITH_REFERENCES(ARRAY) TO"
            + " APPLICATION ROLE ADMIN");
    executeInApp(
        "GRANT USAGE ON PROCEDURE PUBLIC.SETUP_EXTERNAL_INTEGRATION(VARCHAR, VARCHAR, ARRAY) TO"
            + " APPLICATION ROLE ADMIN");
    SharedObjects.createSharedObjects(session);
    SharedObjects.addGrantsOnSharedObjectsToApplication(session, application.instanceName);
    setupSecretReference();
    setupEAIReference();
  }

  @Test
  void shouldAlterGivenProceduresWithEAIAndSecretPassedAsArguments() {
    // when
    var procedureCallResult =
        SqlTools.callProcedure(
            session,
            "PUBLIC",
            "SETUP_EXTERNAL_INTEGRATION",
            asVarchar(TEST_GH_EAI.getValue()),
            asVarchar(TEST_SECRET.getValue()),
            arrayConstruct(asVarchar("PUBLIC.CONFIGURE_CONNECTOR(VARIANT)")));

    // then
    var descResult =
        procedureDescriptor.describeProcedure(
            ObjectName.from("PUBLIC", "CONFIGURE_CONNECTOR"), "VARIANT");
    assertThat(procedureCallResult).hasOKResponseCode();
    assertThat(descResult.getSecrets()).contains(TEST_SECRET.getValue());
    assertThat(descResult.getExternalAccessIntegrations()).contains(TEST_GH_EAI.getValue());
  }

  @Test
  void shouldAlterGivenProceduresWithEAIAndSecretAsPredefinedReferences() {
    // given
    String eaiReference = "reference('EAI_REFERENCE')";
    String secretReference = "reference('SECRET_REFERENCE')";

    // when
    var procedureCallResult =
        SqlTools.callProcedure(
            session,
            "PUBLIC",
            "SETUP_EXTERNAL_INTEGRATION_WITH_REFERENCES",
            arrayConstruct(asVarchar("PUBLIC.CONFIGURE_CONNECTOR(VARIANT)")));

    // then
    var descResult =
        procedureDescriptor.describeProcedure(
            ObjectName.from("PUBLIC", "CONFIGURE_CONNECTOR"), "VARIANT");
    assertThat(procedureCallResult).hasOKResponseCode();
    assertThat(descResult.getSecrets()).contains(secretReference);
    assertThat(descResult.getExternalAccessIntegrations()).contains(eaiReference);
  }

  @Test
  void shouldAlterGivenProceduresWithEAIAndSecretIdentifiersFetchedFromConnectionConfig() {
    // given
    executeInApp(
        String.format(
            "INSERT INTO STATE.APP_CONFIG "
                + "SELECT 'connection_configuration', "
                + "OBJECT_CONSTRUCT('secret', '%s', 'external_access_integration', '%s'), "
                + "'%s'",
            TEST_SECRET.getValue(), TEST_GH_EAI.getValue(), Timestamp.from(Instant.now())));

    // when
    var procedureCallResult =
        SqlTools.callProcedure(
            session,
            "PUBLIC",
            "SETUP_EXTERNAL_INTEGRATION_WITH_NAMES",
            arrayConstruct(asVarchar("PUBLIC.SET_CONNECTION_CONFIGURATION(VARIANT)")));

    // then
    var descResult =
        procedureDescriptor.describeProcedure(
            ObjectName.from("PUBLIC", "SET_CONNECTION_CONFIGURATION"), "VARIANT");
    assertThat(procedureCallResult).hasOKResponseCode();
    assertThat(descResult.getSecrets()).contains(TEST_SECRET.getValue());
    assertThat(descResult.getExternalAccessIntegrations()).contains(TEST_GH_EAI.getValue());
  }

  @Test
  void shouldReturnErrorResponseWhenEAIDoesNotExist() {
    // given
    String nonExistentEAI = "NON_EXISTENT_EAI_33241";

    // when
    var procedureCallResult =
        SqlTools.callProcedure(
            session,
            "PUBLIC",
            "SETUP_EXTERNAL_INTEGRATION",
            asVarchar(nonExistentEAI),
            asVarchar(TEST_SECRET.getValue()),
            arrayConstruct(asVarchar("PUBLIC.CONFIGURE_CONNECTOR(VARIANT)")));

    // then
    assertThat(procedureCallResult).hasResponseCode("EAI_UNAVAILABLE");
    assertThat(procedureCallResult)
        .hasMessage(
            "Unable to apply SECRET and EXTERNAL_OBJECT_INTEGRATION objects to provided methods.");
  }

  @Test
  void shouldReturnErrorResponseWhenSecretDoesNotExist() {
    // given
    String nonExistentSecret = "NON_EXISTENT_SECRET_33241";

    // when
    var procedureCallResult =
        SqlTools.callProcedure(
            session,
            "PUBLIC",
            "SETUP_EXTERNAL_INTEGRATION",
            asVarchar(TEST_GH_EAI.getValue()),
            asVarchar(nonExistentSecret),
            arrayConstruct(asVarchar("PUBLIC.CONFIGURE_CONNECTOR(VARIANT)")));

    // then
    assertThat(procedureCallResult).hasResponseCode("SECRET_UNAVAILABLE");
    assertThat(procedureCallResult)
        .hasMessage(
            "Unable to apply SECRET and EXTERNAL_OBJECT_INTEGRATION objects to provided methods.");
  }
}
