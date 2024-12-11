package com.snowflake.connectors.common.procedure;

import com.snowflake.connectors.common.object.ObjectName;

/** Object representing properties of a stored procedure. */
public class ProcedureProperties {

  private final ObjectName procedureName;
  private final String externalAccessIntegrations;
  private final String secrets;

  /**
   * Creates a new {@link ProcedureProperties}.
   *
   * @param procedureName name of the stored procedure object
   * @param externalAccessIntegrations EAIs assigned to the procedure
   * @param secrets secrets assigned to the procedure
   */
  public ProcedureProperties(
      ObjectName procedureName, String externalAccessIntegrations, String secrets) {
    this.procedureName = procedureName;
    this.externalAccessIntegrations = externalAccessIntegrations;
    this.secrets = secrets;
  }

  /**
   * Returns the procedure name.
   *
   * @return procedure name
   */
  public ObjectName getProcedureName() {
    return procedureName;
  }

  /**
   * Returns the EAIs assigned to the procedure.
   *
   * @return EAIs assigned to the procedure
   */
  public String getExternalAccessIntegrations() {
    return externalAccessIntegrations;
  }

  /**
   * Returns the secrets assigned to the procedure.
   *
   * @return secrets assigned to the procedure
   */
  public String getSecrets() {
    return secrets;
  }
}
