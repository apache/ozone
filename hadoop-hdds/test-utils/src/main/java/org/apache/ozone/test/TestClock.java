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

package org.apache.ozone.test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;

/**
 * An implementation of Clock which allows the time to be set to an instant and
 * moved forward and back. Intended for use only in tests.
 */

public class TestClock extends Clock {

  private Instant instant;
  private final ZoneId zoneId;

  public static TestClock newInstance() {
    return new TestClock(Instant.now(), ZoneOffset.UTC);
  }

  public TestClock(Instant instant, ZoneId zone) {
    this.instant = instant;
    this.zoneId = zone;
  }

  @Override
  public ZoneId getZone() {
    return zoneId;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return new TestClock(Instant.now(), zone);
  }

  @Override
  public Instant instant() {
    return instant;
  }

  public void fastForward(long millis) {
    set(instant().plusMillis(millis));
  }

  public void fastForward(TemporalAmount temporalAmount) {
    set(instant().plus(temporalAmount));
  }

  public void rewind(long millis) {
    set(instant().minusMillis(millis));
  }

  public void rewind(TemporalAmount temporalAmount) {
    set(instant().minus(temporalAmount));
  }

  public void set(Instant newInstant) {
    this.instant = newInstant;
  }

}
