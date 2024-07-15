/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.log;

import org.slf4j.Logger;
import org.slf4j.Marker;

/** Logger which adds a given prefix to all logged messages */
public class PrefixLogger implements Logger {

  private final String prefix;
  private final Logger logger;

  public PrefixLogger(String prefix, Logger logger) {
    this.prefix = prefix;
    this.logger = logger;
  }

  @Override
  public String getName() {
    return logger.getName();
  }

  @Override
  public boolean isTraceEnabled() {
    return logger.isTraceEnabled();
  }

  @Override
  public void trace(String s) {
    logger.trace(prefix + s);
  }

  @Override
  public void trace(String s, Object o) {
    logger.trace(prefix + s, o);
  }

  @Override
  public void trace(String s, Object o, Object o1) {
    logger.trace(prefix + s, o, o);
  }

  @Override
  public void trace(String s, Object... objects) {
    logger.trace(prefix + s, objects);
  }

  @Override
  public void trace(String s, Throwable throwable) {
    logger.trace(prefix + s, throwable);
  }

  @Override
  public boolean isTraceEnabled(Marker marker) {
    return logger.isTraceEnabled();
  }

  @Override
  public void trace(Marker marker, String s) {
    logger.trace(marker, prefix + s);
  }

  @Override
  public void trace(Marker marker, String s, Object o) {
    logger.trace(marker, prefix + s, o);
  }

  @Override
  public void trace(Marker marker, String s, Object o, Object o1) {
    logger.trace(marker, prefix + s, o, o1);
  }

  @Override
  public void trace(Marker marker, String s, Object... objects) {
    logger.trace(marker, prefix + s, objects);
  }

  @Override
  public void trace(Marker marker, String s, Throwable throwable) {
    logger.trace(marker, prefix + s, throwable);
  }

  @Override
  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }

  @Override
  public void debug(String s) {
    logger.debug(prefix + s);
  }

  @Override
  public void debug(String s, Object o) {
    logger.debug(prefix + s, o);
  }

  @Override
  public void debug(String s, Object o, Object o1) {
    logger.debug(prefix + s, o, o1);
  }

  @Override
  public void debug(String s, Object... objects) {
    logger.debug(prefix + s, objects);
  }

  @Override
  public void debug(String s, Throwable throwable) {
    logger.debug(prefix + s, throwable);
  }

  @Override
  public boolean isDebugEnabled(Marker marker) {
    return logger.isDebugEnabled(marker);
  }

  @Override
  public void debug(Marker marker, String s) {
    logger.debug(marker, prefix + s);
  }

  @Override
  public void debug(Marker marker, String s, Object o) {
    logger.debug(marker, prefix + s, o);
  }

  @Override
  public void debug(Marker marker, String s, Object o, Object o1) {
    logger.debug(marker, prefix + s, o, o1);
  }

  @Override
  public void debug(Marker marker, String s, Object... objects) {
    logger.debug(marker, prefix + s, objects);
  }

  @Override
  public void debug(Marker marker, String s, Throwable throwable) {
    logger.debug(marker, prefix + s, throwable);
  }

  @Override
  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  @Override
  public void info(String s) {
    logger.info(prefix + s);
  }

  @Override
  public void info(String s, Object o) {
    logger.info(prefix + s, o);
  }

  @Override
  public void info(String s, Object o, Object o1) {
    logger.info(prefix + s, o, o1);
  }

  @Override
  public void info(String s, Object... objects) {
    logger.info(prefix + s, objects);
  }

  @Override
  public void info(String s, Throwable throwable) {
    logger.info(prefix + s, throwable);
  }

  @Override
  public boolean isInfoEnabled(Marker marker) {
    return logger.isInfoEnabled(marker);
  }

  @Override
  public void info(Marker marker, String s) {
    logger.info(marker, prefix + s);
  }

  @Override
  public void info(Marker marker, String s, Object o) {
    logger.info(marker, prefix + s, o);
  }

  @Override
  public void info(Marker marker, String s, Object o, Object o1) {
    logger.info(marker, prefix + s, o, o1);
  }

  @Override
  public void info(Marker marker, String s, Object... objects) {
    logger.info(marker, prefix + s, objects);
  }

  @Override
  public void info(Marker marker, String s, Throwable throwable) {
    logger.info(marker, prefix + s, throwable);
  }

  @Override
  public boolean isWarnEnabled() {
    return logger.isWarnEnabled();
  }

  @Override
  public void warn(String s) {
    logger.warn(prefix + s);
  }

  @Override
  public void warn(String s, Object o) {
    logger.warn(prefix + s, o);
  }

  @Override
  public void warn(String s, Object... objects) {
    logger.warn(prefix + s, objects);
  }

  @Override
  public void warn(String s, Object o, Object o1) {
    logger.warn(prefix + s, o, o1);
  }

  @Override
  public void warn(String s, Throwable throwable) {
    logger.warn(prefix + s, throwable);
  }

  @Override
  public boolean isWarnEnabled(Marker marker) {
    return logger.isInfoEnabled(marker);
  }

  @Override
  public void warn(Marker marker, String s) {
    logger.warn(marker, prefix + s);
  }

  @Override
  public void warn(Marker marker, String s, Object o) {
    logger.warn(marker, prefix + s, o);
  }

  @Override
  public void warn(Marker marker, String s, Object o, Object o1) {
    logger.warn(marker, prefix + s, o, o1);
  }

  @Override
  public void warn(Marker marker, String s, Object... objects) {
    logger.warn(marker, prefix + s, objects);
  }

  @Override
  public void warn(Marker marker, String s, Throwable throwable) {
    logger.warn(marker, prefix + s, throwable);
  }

  @Override
  public boolean isErrorEnabled() {
    return logger.isErrorEnabled();
  }

  @Override
  public void error(String s) {
    logger.error(prefix + s);
  }

  @Override
  public void error(String s, Object o) {
    logger.error(prefix + s, o);
  }

  @Override
  public void error(String s, Object o, Object o1) {
    logger.error(prefix + s, o, o1);
  }

  @Override
  public void error(String s, Object... objects) {
    logger.error(prefix + s, objects);
  }

  @Override
  public void error(String s, Throwable throwable) {
    logger.error(prefix + s, throwable);
  }

  @Override
  public boolean isErrorEnabled(Marker marker) {
    return logger.isErrorEnabled(marker);
  }

  @Override
  public void error(Marker marker, String s) {
    logger.error(marker, prefix + s);
  }

  @Override
  public void error(Marker marker, String s, Object o) {
    logger.error(marker, prefix + s, o);
  }

  @Override
  public void error(Marker marker, String s, Object o, Object o1) {
    logger.error(marker, prefix + s, o, o1);
  }

  @Override
  public void error(Marker marker, String s, Object... objects) {
    logger.error(marker, prefix + s, objects);
  }

  @Override
  public void error(Marker marker, String s, Throwable throwable) {
    logger.error(marker, prefix + s, throwable);
  }
}
