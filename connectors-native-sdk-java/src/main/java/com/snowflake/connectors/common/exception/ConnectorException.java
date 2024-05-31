/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.common.exception;

import com.snowflake.connectors.common.exception.context.EmptyExceptionContext;
import com.snowflake.connectors.common.exception.context.ExceptionContext;
import com.snowflake.connectors.common.response.ConnectorResponse;

/**
 * Main exception for the connector errors. It is recommended that this class is extended and custom
 * exceptions are created and thrown.
 *
 * <p>The message of this exception is derived from the provided exception message or a message of
 * the provided response.
 *
 * <p>For more explanation about the response and context properties see {@link ConnectorResponse}
 * and {@link ExceptionContext}.
 */
public class ConnectorException extends RuntimeException {

  private final ConnectorResponse response;
  private final ExceptionContext context;

  /**
   * Creates a new {@link ConnectorException}, with a provided response code, message and an empty
   * exception context.
   *
   * @param responseCode exception response code
   * @param message exception message
   */
  public ConnectorException(String responseCode, String message) {
    this(responseCode, message, EmptyExceptionContext.INSTANCE);
  }

  /**
   * Creates a new {@link ConnectorException}, with a provided response and an empty exception
   * context.
   *
   * @param response exception response
   */
  public ConnectorException(ConnectorResponse response) {
    this(response, EmptyExceptionContext.INSTANCE);
  }

  /**
   * Creates a new {@link ConnectorException}, with a provided response code, message and exception
   * context.
   *
   * @param responseCode exception response code
   * @param message exception message
   * @param context exception context
   */
  public ConnectorException(String responseCode, String message, ExceptionContext context) {
    super(message);
    this.response = ConnectorResponse.error(responseCode, message);
    this.context = context;
  }

  /**
   * Creates a new {@link ConnectorException}, with a provided response and exception context.
   *
   * @param response exception response
   * @param context exception context
   */
  public ConnectorException(ConnectorResponse response, ExceptionContext context) {
    super(response.getMessage());
    this.response = response;
    this.context = context;
  }

  /**
   * Creates a new {@link ConnectorException}, with a provided response code, message, an empty
   * exception context and an exception cause.
   *
   * @param responseCode exception response code
   * @param message exception message
   * @param cause exception cause
   */
  public ConnectorException(String responseCode, String message, Throwable cause) {
    this(responseCode, message, EmptyExceptionContext.INSTANCE, cause);
  }

  /**
   * Creates a new {@link ConnectorException}, with a provided response and an exception cause.
   *
   * @param response exception response
   * @param cause exception cause
   */
  public ConnectorException(ConnectorResponse response, Throwable cause) {
    this(response, EmptyExceptionContext.INSTANCE, cause);
  }

  /**
   * Creates a new {@link ConnectorException}, with a provided response code, message, exception
   * context and an exception cause.
   *
   * @param responseCode exception response code
   * @param message exception message
   * @param context exception context
   * @param cause exception cause
   */
  public ConnectorException(
      String responseCode, String message, ExceptionContext context, Throwable cause) {
    super(message, cause);
    this.response = ConnectorResponse.error(responseCode, message);
    this.context = context;
  }

  /**
   * Creates a new {@link ConnectorException}, with a provided response, exception context and an
   * exception cause.
   *
   * @param response exception response
   * @param context exception context
   * @param cause exception cause
   */
  public ConnectorException(ConnectorResponse response, ExceptionContext context, Throwable cause) {
    super(response.getMessage(), cause);
    this.response = response;
    this.context = context;
  }

  /**
   * Connector response of this exception.
   *
   * @return underlying response of this exception
   */
  public ConnectorResponse getResponse() {
    return response;
  }

  /**
   * Context of this exception.
   *
   * @return context of this exception
   */
  public ExceptionContext getContext() {
    return context;
  }
}
