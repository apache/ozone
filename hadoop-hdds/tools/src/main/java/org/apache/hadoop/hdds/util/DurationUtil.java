/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.util;

/**
 * Pretty duration string representation.
 */
public final class DurationUtil {

  private DurationUtil() {
  }

  public static String getPrettyDuration(long durationSeconds) {
    String prettyDuration;
    if (durationSeconds >= 0 && durationSeconds < 60) {
      prettyDuration = durationSeconds + "s";
    } else if (durationSeconds >= 60 && durationSeconds < 3600) {
      prettyDuration = (durationSeconds / 60 + "m " + durationSeconds % 60 + "s");
    } else if (durationSeconds >= 3600) {
      prettyDuration =
          (durationSeconds / 60 / 60 + "h " + durationSeconds / 60 % 60 + "m " + durationSeconds % 60 + "s");
    } else {
      throw new IllegalStateException("Incorrect duration exception" + durationSeconds);
    }
    return prettyDuration;
  }
}
