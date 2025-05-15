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
    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    if (zonedDateTime != null) {
      return now.isAfter(zonedDateTime);
    } else {
      ZonedDateTime dateTime =
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp + daysInMilli), ZoneOffset.UTC);
      return now.isBefore(dateTime);
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
   * @throws OMException if the validation fails
   */
  @Override
  public void valid() throws OMException {
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
      validateExpirationDate(date);
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
   * @throws OMException if the date is invalid
   */
  private void validateExpirationDate(String expirationDate) throws OMException {
    try {
      ZonedDateTime parsedDate = ZonedDateTime.parse(expirationDate, DateTimeFormatter.ISO_DATE_TIME);
      ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
      // Convert to UTC for validation
      ZonedDateTime dateInUTC = parsedDate.withZoneSameInstant(ZoneOffset.UTC);
      // The date value must conform to the ISO 8601 format, be in the future.
      if (dateInUTC.isBefore(now)) {
        throw new OMException("Invalid lifecycle configuration: 'Date' must be in the future " + now + "," + dateInUTC,
            OMException.ResultCodes.INVALID_REQUEST);
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

  public static OmLCExpiration getFromProtobuf(
      LifecycleExpiration lifecycleExpiration) throws OMException {
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

    public OmLCExpiration build() throws OMException {
      OmLCExpiration omLCExpiration = new OmLCExpiration(this);
      omLCExpiration.valid();
      return omLCExpiration;
    }
  }

  public static void setTest(boolean isTest) {
    test = isTest;
  }
}
