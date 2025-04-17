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

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import org.apache.hadoop.ozone.om.exceptions.OMException;

/**
 * A class that encapsulates lifecycle rule expiration action.
 * This class extends OmLCAction and represents the expiration
 * action type in lifecycle configuration.
 */
public class OmLCExpiration implements OmLCAction {

  private int days;
  private String date;

  OmLCExpiration(int days, String date) {
    this.days = days;
    this.date = date;
  }

  public int getDays() {
    return days;
  }

  public void setDays(int days) {
    this.days = days;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  @Override
  public ActionType getActionType() {
    return ActionType.EXPIRATION;
  }

  /**
   * Validates the expiration configuration.
   * - Days must be a positive number greater than zero
   * - Either days or date should be specified, but not both or neither
   * - The date value must conform to the ISO 8601 format
   * - The date value must be in the future
   * - The date value must be at midnight UTC (00:00:00Z)
   *
   * @throws OMException if the validation fails
   */
  @Override
  public void valid() throws OMException {
    if (days < 0) {
      throw new OMException("'Days' for Expiration action must be a positive integer.",
          OMException.ResultCodes.INVALID_REQUEST);
    }

    boolean hasDays = days > 0;
    boolean hasDate = date != null && !date.isEmpty();

    if (hasDays == hasDate) {
      throw new OMException("Invalid lifecycle configuration: Either 'days' or 'date' " +
          "should be specified, but not both or neither.", OMException.ResultCodes.INVALID_REQUEST);
    }

    // The date value must conform to the ISO 8601 format, be in the future, and be at midnight UTC
    if (hasDate) {
      try {
        ZonedDateTime parsedDate = ZonedDateTime.parse(date, DateTimeFormatter.ISO_DATE_TIME);
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);

        if (parsedDate.isBefore(now)) {
          throw new OMException("Invalid lifecycle configuration: 'Date' must be in the future",
              OMException.ResultCodes.INVALID_REQUEST);
        }

        // Verify that the time is midnight UTC (00:00:00Z)
        if (parsedDate.getHour() != 0 || parsedDate.getMinute() != 0 || parsedDate.getSecond() != 0
            || parsedDate.getNano() != 0 || !parsedDate.getZone().equals(ZoneOffset.UTC)) {
          throw new OMException("Invalid lifecycle configuration: 'Date' must be at midnight GMT",
              OMException.ResultCodes.INVALID_REQUEST);
        }
      } catch (DateTimeParseException ex) {
        throw new OMException("Invalid lifecycle configuration: 'Date' must be in ISO 8601 format",
            OMException.ResultCodes.INVALID_REQUEST);
      }
    }
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
    private int days;
    private String date = "";

    public Builder setDays(int lcDays) {
      this.days = lcDays;
      return this;
    }

    public Builder setDate(String lcDate) {
      this.date = lcDate;
      return this;
    }

    public OmLCExpiration build() {
      return new OmLCExpiration(days, date);
    }
  }
}
