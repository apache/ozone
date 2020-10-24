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
package org.apache.hadoop.hdds.ratis.conf;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link RatisClientConfig}.
 */
class TestRatisClientConfig {

  @Test
  void defaults() {
    RatisClientConfig fromConfig = new OzoneConfiguration()
        .getObject(RatisClientConfig.class);

    RatisClientConfig subject = new RatisClientConfig();

    assertEquals(fromConfig.getExponentialPolicyBaseSleep(),
        subject.getExponentialPolicyBaseSleep());
    assertEquals(fromConfig.getExponentialPolicyMaxSleep(),
        subject.getExponentialPolicyMaxSleep());
    assertEquals(fromConfig.getWatchRequestTimeout(),
        subject.getWatchRequestTimeout());
    assertEquals(fromConfig.getWriteRequestTimeout(),
        subject.getWriteRequestTimeout());
  }

  @Test
  void setAndGet() {
    RatisClientConfig subject = new RatisClientConfig();
    final Duration baseSleep = Duration.ofSeconds(12);
    final Duration maxSleep = Duration.ofMinutes(2);
    final Duration watchRequestTimeout = Duration.ofMillis(555);
    final Duration writeRequestTimeout = Duration.ofMillis(444);

    subject.setExponentialPolicyBaseSleep(baseSleep);
    subject.setExponentialPolicyMaxSleep(maxSleep);
    subject.setWatchRequestTimeout(watchRequestTimeout);
    subject.setWriteRequestTimeout(writeRequestTimeout);

    assertEquals(baseSleep, subject.getExponentialPolicyBaseSleep());
    assertEquals(maxSleep, subject.getExponentialPolicyMaxSleep());
    assertEquals(watchRequestTimeout, subject.getWatchRequestTimeout());
    assertEquals(writeRequestTimeout, subject.getWriteRequestTimeout());
  }

}
