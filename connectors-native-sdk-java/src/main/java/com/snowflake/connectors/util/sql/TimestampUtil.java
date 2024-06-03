/** Copyright (c) 2024 Snowflake Inc. */
package com.snowflake.connectors.util.sql;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Utility class which provides mapping methods between {@link Timestamp} and {@link Instant}.
 *
 * <p>The mapping methods should be used in order to properly save or fetch a UTC timestamp value
 * from a TIMESTAMP_NTZ column.
 *
 * <p>The reason for using the methods is that Snowflake treats Timestamp as a date in your local
 * time zone, and when the conversion is not handled properly - the information about the time zone
 * can be lost, which leads to incorrect values saved in database or fetched from database.
 */
public class TimestampUtil {

  private TimestampUtil() {}

  /**
   * Method for converting given Timestamp to Instant. This method should be used in order to keep
   * given date in UTC format when it is needed to convert a Timestamp fetched from database to
   * Instant. It should be used instead of Timestamp#toInstant, because Snowflake treats Timestamp
   * object as a date in your local time zone and when you use Timestamp#toInstant() - the
   * information about the time zone is being lost.
   *
   * @param timestamp timestamp object in your local time zone
   * @return instant object in UTC time zone
   */
  public static Instant toInstant(Timestamp timestamp) {
    if (timestamp == null) {
      return null;
    }

    return timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC);
  }

  /**
   * Method for converting given Instant to Timestamp. The method converts a UTC Instant to a
   * Timestamp in your local time zone, so that when this value is saved to a database to a
   * TIMESTAMP_NTZ column, the value will be in UTC format.
   *
   * @param instant instant object in UTC format
   * @return timestamp object in your local time zone
   */
  public static Timestamp toTimestamp(Instant instant) {
    if (instant == null) {
      return null;
    }

    return Timestamp.valueOf(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
  }
}
