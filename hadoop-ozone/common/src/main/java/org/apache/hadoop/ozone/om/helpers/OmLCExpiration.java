/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleAction;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LifecycleExpiration;

/**
 * A class that encapsulates lifecycle rule expiration action.
 * This class extends OmLCAction and represents the expiration
 * action type in lifecycle configuration.
 */
public final class OmLCExpiration implements OmLCAction {
  private final Integer days;
  private final String date;
  private ZonedDateTime zonedDateTime;
  private long daysInMilli;
  private static boolean test;

  private OmLCExpiration() {
    throw new UnsupportedOperationException("Default constructor is not supported. Use Builder.");
  }

  private OmLCExpiration(Builder builder) {
    this.days = builder.days;
    this.date = builder.date;
  }

  @Nullable
  public Integer getDays() {
    return days;
  }

  @Nullable
  public String getDate() {
    return date;
  }

  public boolean isExpired(long timestamp) {
    if (zonedDateTime != null) {
      Instant instant = Instant.ofEpochMilli(timestamp);
      ZonedDateTime objectTime = instant.atZone(ZoneOffset.UTC);
      return objectTime.isBefore(zonedDateTime);
    } else {
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      ZonedDateTime dateTime =
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp + daysInMilli), ZoneOffset.UTC);
      return now.isAfter(dateTime);
    }
  }

  @Override
  public ActionType getActionType() {
    return ActionType.EXPIRATION;
  }

  /**
   * Validates the expiration configuration.
   * - Days must be a positive number greater than zero if set
   * - Either days or date should be specified, but not both or neither
   * - The date value must conform to the ISO 8601 format
   * - The date value must be in the future
   * - The date value must be at midnight UTC (00:00:00Z)
   *
   * @param creationTime The creation time of the lifecycle configuration in milliseconds since epoch
   * @throws OMException if the validation fails
   */
  @Override
  public void valid(long creationTime) throws OMException {
    boolean hasDays = days != null;
    boolean hasDate = !StringUtils.isBlank(date);

    if (hasDays == hasDate) {
      throw new OMException("Invalid lifecycle configuration: Either 'days' or 'date' " +
          "should be specified, but not both or neither.", OMException.ResultCodes.INVALID_REQUEST);
    }
    if (hasDays) {
      if (days <= 0) {
        throw new OMException("'Days' for Expiration action must be a positive integer greater than zero.",
            OMException.ResultCodes.INVALID_REQUEST);
      }
      daysInMilli = TimeUnit.DAYS.toMillis(days);
    }
    if (hasDate) {
      validateExpirationDate(date, creationTime);
    }
  }

  /**
   * Validates that the expiration date is:
   * - In the ISO 8601 format
   * - Includes both time and time zone (neither can be omitted)
   * - In the future
   * - Represents midnight UTC (00:00:00Z) when converted to UTC.
   *
   * @param expirationDate The date string to validate
   * @param creationTime The creation time to compare against in milliseconds since epoch
   * @throws OMException if the date is invalid
   */
  private void validateExpirationDate(String expirationDate, long creationTime) throws OMException {
    try {
      ZonedDateTime parsedDate = ZonedDateTime.parse(expirationDate, DateTimeFormatter.ISO_DATE_TIME);
      // Convert to UTC for validation
      ZonedDateTime dateInUTC = parsedDate.withZoneSameInstant(ZoneOffset.UTC);
      // The date value must conform to the ISO 8601 format, be in the future.
      ZonedDateTime createDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(creationTime), ZoneOffset.UTC);
      if (dateInUTC.isBefore(createDate)) {
        throw new OMException("Invalid lifecycle configuration: 'Date' must be in the future " + createDate + "," +
            dateInUTC, OMException.ResultCodes.INVALID_REQUEST);
      }

      // Verify that the time is midnight UTC (00:00:00Z)
      if (!test && (dateInUTC.getHour() != 0 ||
          dateInUTC.getMinute() != 0 ||
          dateInUTC.getSecond() != 0 ||
          dateInUTC.getNano() != 0)) {
        throw new OMException("Invalid lifecycle configuration: 'Date' must represent midnight UTC (00:00:00Z). " +
            "Examples: '2042-04-02T00:00:00Z' or '2042-04-02T00:00:00+00:00'",
            OMException.ResultCodes.INVALID_REQUEST);
      }
      zonedDateTime = parsedDate;
    } catch (DateTimeParseException ex) {
      throw new OMException("Invalid lifecycle configuration: 'Date' must be in ISO 8601 format with " +
          "time and time zone included. Examples: '2042-04-02T00:00:00Z' or '2042-04-02T00:00:00+00:00'",
          OMException.ResultCodes.INVALID_REQUEST);
    }
  }

  @Override
  public LifecycleAction getProtobuf() {
    LifecycleExpiration.Builder builder = LifecycleExpiration.newBuilder();

    if (date != null) {
      builder.setDate(date);
    }
    if (days != null) {
      builder.setDays(days);
    }

    return LifecycleAction.newBuilder().setExpiration(builder).build();
  }

  public static OmLCExpiration getFromProtobuf(LifecycleExpiration lifecycleExpiration) {
    OmLCExpiration.Builder builder = new Builder();

    if (lifecycleExpiration.hasDate()) {
      builder.setDate(lifecycleExpiration.getDate());
    }
    if (lifecycleExpiration.hasDays()) {
      builder.setDays(lifecycleExpiration.getDays());
    }

    return builder.build();
  }

  @Override
  public String toString() {
    return "OmLCExpiration{" +
        "days=" + days +
        ", date='" + date + '\'' +
        '}';
  }

  /**
   * Builder of OmLCExpiration.
   */
  public static class Builder {
    private Integer days = null;
    private String date = null;

    public Builder setDays(int lcDays) {
      this.days = lcDays;
      return this;
    }

    public Builder setDate(String lcDate) {
      this.date = lcDate;
      return this;
    }

    public OmLCExpiration build() {
      return new OmLCExpiration(this);
    }
  }

  @VisibleForTesting
  public static void setTest(boolean isTest) {
    test = isTest;
  }
}
