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

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RetentionTimeUnit;

/**
 * Validation and UTC date calculations for retention rules.
 */
public final class RetentionUtil {
  private static final long MAX_EVENT_HOLD_YEARS = 100;
  private static final long DAYS_PER_YEAR = 365;
  private static final long MAX_EVENT_HOLD_DAYS = MAX_EVENT_HOLD_YEARS * DAYS_PER_YEAR;

  private RetentionUtil() {
  }

  public static void validateBucketRetention(BucketRetention retention) {
    RetentionDuration duration = retention.getDuration();
    RetentionDuration eventHold = retention.getDefaultEventHold();
    if (duration == null && eventHold == null) {
      throw new IllegalArgumentException("Default retention requires a duration or event hold");
    }
    if (duration != null) {
      validateRule(duration);
    }
    if (eventHold != null) {
      validateEventHoldRule(eventHold);
      if (duration != null && exceedsDefaultDuration(eventHold, duration)) {
        throw new IllegalArgumentException("Event hold duration must not exceed default retention duration");
      }
    }
  }

  private static boolean exceedsDefaultDuration(RetentionDuration eventHold, RetentionDuration duration) {
    long hold = eventHold.getDuration();
    long defaultDuration = duration.getDuration();
    if (eventHold.getTimeUnit() == duration.getTimeUnit()) {
      return hold > defaultDuration;
    }
    if (eventHold.getTimeUnit() == RetentionTimeUnit.YEARS) {
      // At most one leap day per four years, rounded up for the longest UTC interval.
      return hold * DAYS_PER_YEAR + (hold + 3) / 4 > defaultDuration;
    }
    // A default of 100 years already covers any hold in days. Capping also avoids overflow.
    long years = Math.min(defaultDuration, MAX_EVENT_HOLD_YEARS);
    // The shortest interval can lose one leap day at a non-leap century.
    return hold > years * DAYS_PER_YEAR + Math.max(0, years / 4 - 1);
  }

  /**
   * Validate retention submitted by a write request at the given time in epoch milliseconds.
   */
  public static void validateObjectRetention(ObjectRetention retention, long currentTime) {
    long retentionDate = retention.getRetentionDate();
    EventHold eventHold = retention.getEventHold();
    if (retentionDate == 0 && eventHold == null) {
      throw new IllegalArgumentException("Retention requires an expiration time or event hold");
    }
    if (retentionDate != 0) {
      validateRetainUntilDate(retentionDate, currentTime);
    }
    if (eventHold != null) {
      validateEventHoldRule(eventHold.getDuration());
      if (eventHold.getEnabled() && retentionDate != 0
          && retentionDate < calculateRetainUntilDate(eventHold.getDuration(), currentTime)) {
        throw new IllegalArgumentException("Retention expiration must be at least the event hold duration from now");
      }
    }
  }

  private static void validateRule(RetentionDuration rule) {
    if (rule == null || rule.getDuration() <= 0) {
      throw new IllegalArgumentException("Retention rule requires a time unit and positive signed-long duration");
    }
  }

  private static void validateEventHoldRule(RetentionDuration rule) {
    validateRule(rule);
    switch (rule.getTimeUnit()) {
    case DAYS:
      if (rule.getDuration() <= MAX_EVENT_HOLD_DAYS) {
        return;
      }
      break;
    case YEARS:
      if (rule.getDuration() <= MAX_EVENT_HOLD_YEARS) {
        return;
      }
      break;
    default:
      throw new IllegalArgumentException("Unsupported retention time unit: " + rule.getTimeUnit());
    }
    throw new IllegalArgumentException("Event hold duration must not exceed 36,500 days or 100 years");
  }

  /**
   * Calculate an expiration time in epoch milliseconds from an explicit start time.
   * Event hold release and selection of the applicable rule are the caller's responsibility.
   */
  public static long calculateRetainUntilDate(RetentionDuration rule, long startTime) {
    validateRule(rule);
    if (startTime < 0) {
      throw new IllegalArgumentException("Retention start time must not be negative");
    }
    try {
      switch (rule.getTimeUnit()) {
      case DAYS:
        return Math.addExact(startTime, Math.multiplyExact(rule.getDuration(), 86_400_000L));
      case YEARS:
        return Instant.ofEpochMilli(startTime).atZone(ZoneOffset.UTC).plusYears(rule.getDuration())
            .toInstant().toEpochMilli();
      default:
        throw new IllegalArgumentException("Unsupported retention time unit: " + rule.getTimeUnit());
      }
    } catch (ArithmeticException | DateTimeException ex) {
      throw new IllegalArgumentException("Retention expiration time is out of range", ex);
    }
  }

  private static void validateRetainUntilDate(long retentionDate, long currentTime) {
    if (retentionDate < 0 || retentionDate <= currentTime) {
      throw new IllegalArgumentException("Retention expiration time must be in the future");
    }
  }
}
