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
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Rule;

/**
 * Validation and UTC date calculations for retention rules.
 */
public final class RetentionUtil {
  private RetentionUtil() {
  }

  public static void validateRule(Rule rule) {
    if (rule == null || !rule.isInitialized() || rule.getDuration() <= 0) {
      throw new IllegalArgumentException("Retention requires a mode, time unit and positive signed-long duration");
    }
  }

  /**
   * Calculate an expiration time in epoch milliseconds from an explicit start time.
   * Event hold release and selection of the applicable rule are the caller's responsibility.
   */
  public static long calculateRetainUntilDate(Rule rule, long startTime) {
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

  public static void validateRetainUntilDate(long retentionDate, long currentTime) {
    if (retentionDate < 0 || retentionDate <= currentTime) {
      throw new IllegalArgumentException("Retention expiration time must be in the future");
    }
  }
}
