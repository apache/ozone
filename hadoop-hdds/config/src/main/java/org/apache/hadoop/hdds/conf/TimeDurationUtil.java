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

package org.apache.hadoop.hdds.conf;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to handle time duration.
 */
public final class TimeDurationUtil {

  private static final Logger LOG =
      LoggerFactory.getLogger(TimeDurationUtil.class);

  private TimeDurationUtil() {
  }

  /**
   * Return time duration in the given time unit. Valid units are encoded in
   * properties as suffixes: nanoseconds (ns), microseconds (us), milliseconds
   * (ms), seconds (s), minutes (m), hours (h), and days (d).
   *
   * @param name Property name
   * @param vStr The string value with time unit suffix to be converted.
   * @param unit Unit to convert the stored property, if it exists.
   */
  public static long getTimeDurationHelper(String name, String vStr,
      TimeUnit unit) {
    final long millis = getDuration(name, vStr, unit).toMillis();
    return unit.convert(millis, TimeUnit.MILLISECONDS);
  }

  public static Duration getDuration(String name, String vStr, TimeUnit unit) {
    vStr = vStr.trim();
    vStr = vStr.toLowerCase();
    ParsedTimeDuration vUnit = ParsedTimeDuration.unitFor(vStr);
    if (null == vUnit) {
      LOG.warn("No unit for " + name + "(" + vStr + ") assuming " + unit);
      vUnit = ParsedTimeDuration.unitFor(unit);
      if (null == vUnit) {
        throw new IllegalArgumentException("Unexpected unit: " + unit);
      }
    } else {
      vStr = vStr.substring(0, vStr.lastIndexOf(vUnit.suffix()));
    }

    long raw = Long.parseLong(vStr);
    return Duration.of(raw, vUnit.temporalUnit());
  }

  enum ParsedTimeDuration {
    NS {
      @Override
      TimeUnit unit() {
        return TimeUnit.NANOSECONDS;
      }

      @Override
      String suffix() {
        return "ns";
      }

      @Override
      TemporalUnit temporalUnit() {
        return ChronoUnit.NANOS;
      }
    },
    US {
      @Override
      TimeUnit unit() {
        return TimeUnit.MICROSECONDS;
      }

      @Override
      String suffix() {
        return "us";
      }

      @Override
      TemporalUnit temporalUnit() {
        return ChronoUnit.MICROS;
      }
    },
    MS {
      @Override
      TimeUnit unit() {
        return TimeUnit.MILLISECONDS;
      }

      @Override
      String suffix() {
        return "ms";
      }

      @Override
      TemporalUnit temporalUnit() {
        return ChronoUnit.MILLIS;
      }
    },
    S {
      @Override
      TimeUnit unit() {
        return TimeUnit.SECONDS;
      }

      @Override
      String suffix() {
        return "s";
      }

      @Override
      TemporalUnit temporalUnit() {
        return ChronoUnit.SECONDS;
      }
    },
    M {
      @Override
      TimeUnit unit() {
        return TimeUnit.MINUTES;
      }

      @Override
      String suffix() {
        return "m";
      }

      @Override
      TemporalUnit temporalUnit() {
        return ChronoUnit.MINUTES;
      }
    },
    H {
      @Override
      TimeUnit unit() {
        return TimeUnit.HOURS;
      }

      @Override
      String suffix() {
        return "h";
      }

      @Override
      TemporalUnit temporalUnit() {
        return ChronoUnit.HOURS;
      }
    },
    D {
      @Override
      TimeUnit unit() {
        return TimeUnit.DAYS;
      }

      @Override
      String suffix() {
        return "d";
      }

      @Override
      TemporalUnit temporalUnit() {
        return ChronoUnit.DAYS;
      }
    };

    abstract TimeUnit unit();

    abstract String suffix();

    abstract TemporalUnit temporalUnit();

    static ParsedTimeDuration unitFor(String s) {
      for (ParsedTimeDuration ptd : values()) {
        // iteration order is in decl order, so SECONDS matched last
        if (s.endsWith(ptd.suffix())) {
          return ptd;
        }
      }
      return null;
    }

    static ParsedTimeDuration unitFor(TimeUnit unit) {
      for (ParsedTimeDuration ptd : values()) {
        if (ptd.unit() == unit) {
          return ptd;
        }
      }
      return null;
    }
  }
}
