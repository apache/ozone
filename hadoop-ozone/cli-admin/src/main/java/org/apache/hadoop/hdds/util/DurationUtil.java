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

package org.apache.hadoop.hdds.util;

import static java.lang.String.format;

import java.time.Duration;

/**
 * Pretty duration string representation.
 */
public final class DurationUtil {

  private DurationUtil() {
  }

  /**
   * Modify duration to string view. E.x. 1h 30m 45s, 2m 30s, 30s.
   *
   * @param duration duration
   * @return duration in string format
   */
  public static String getPrettyDuration(Duration duration) {
    long hours = duration.toHours();
    long minutes = duration.getSeconds() / 60 % 60;
    long seconds = duration.getSeconds() % 60;
    if (hours > 0) {
      return format("%dh %dm %ds", hours, minutes, seconds);
    } else if (minutes > 0) {
      return format("%dm %ds", minutes, seconds);
    } else if (seconds >= 0) {
      return format("%ds", seconds);
    } else {
      throw new IllegalStateException("Provided duration is incorrect: " + duration);
    }
  }
}
