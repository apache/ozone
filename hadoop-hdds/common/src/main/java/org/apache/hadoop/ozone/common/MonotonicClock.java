/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.common;

import org.apache.hadoop.util.Time;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

/**
 * This is a class which implements the Clock interface. It is a copy of the
 * Java Clock.SystemClock only it uses MonotonicNow (nanotime) rather than
 * System.currentTimeMills.
 */

public final class MonotonicClock extends Clock {

  private final ZoneId zoneId;

  public MonotonicClock(ZoneId zone) {
    this.zoneId = zone;
  }

  @Override
  public ZoneId getZone() {
    return zoneId;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    if (zone.equals(this.zoneId)) {  // intentional NPE
      return this;
    }
    return new MonotonicClock(zone);
  }

  @Override
  public long millis() {
    return Time.monotonicNow();
  }

  @Override
  public Instant instant() {
    return Instant.ofEpochMilli(millis());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MonotonicClock) {
      return zoneId.equals(((MonotonicClock) obj).zoneId);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return zoneId.hashCode() + 1;
  }

  @Override
  public String toString() {
    return "MonotonicClock[" + zoneId + "]";
  }

}
