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

package org.apache.hadoop.hdds.ratis.conf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;

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
    assertEquals(fromConfig.getExponentialPolicyMaxRetries(),
        subject.getExponentialPolicyMaxRetries());
  }

  @Test
  void setAndGet() {
    RatisClientConfig subject = new RatisClientConfig();
    final Duration baseSleep = Duration.ofSeconds(12);
    final Duration maxSleep = Duration.ofMinutes(2);
    final Duration watchRequestTimeout = Duration.ofMillis(555);
    final Duration writeRequestTimeout = Duration.ofMillis(444);
    final int maxRetry = 10;

    subject.setExponentialPolicyBaseSleep(baseSleep);
    subject.setExponentialPolicyMaxSleep(maxSleep);
    subject.setWatchRequestTimeout(watchRequestTimeout);
    subject.setWriteRequestTimeout(writeRequestTimeout);
    subject.setExponentialPolicyMaxRetries(maxRetry);

    assertEquals(baseSleep, subject.getExponentialPolicyBaseSleep());
    assertEquals(maxSleep, subject.getExponentialPolicyMaxSleep());
    assertEquals(watchRequestTimeout, subject.getWatchRequestTimeout());
    assertEquals(writeRequestTimeout, subject.getWriteRequestTimeout());
    assertEquals(maxRetry, subject.getExponentialPolicyMaxRetries());
  }

  /**
   * Regression guard for HDDS-15444: the production defaults must keep the
   * worst-case wall-clock of a single Ratis-client retry cycle bounded.
   * Per cycle = write-rpc + max-retries × (backoff-sleep + write-rpc) +
   * watch-rpc. With the post-HDDS-15444 defaults this is ~213 s; we assert
   * it stays under 4 minutes so a future revert of any one knob is caught
   * in a unit test rather than in a multi-minute integration test.
   */
  @Test
  void defaultsBoundSingleCycleWallClock() {
    RatisClientConfig subject = new OzoneConfiguration()
        .getObject(RatisClientConfig.class);
    RatisClientConfig.RaftConfig raftSubject = new OzoneConfiguration()
        .getObject(RatisClientConfig.RaftConfig.class);

    Duration writeRpc = raftSubject.getRpcRequestTimeout();
    Duration watchRpc = raftSubject.getRpcWatchRequestTimeout();
    int maxRetries = subject.getExponentialPolicyMaxRetries();
    Duration maxBackoff = subject.getExponentialPolicyMaxSleep();

    Duration perCycle = writeRpc
        .plus(maxBackoff.plus(writeRpc).multipliedBy(maxRetries))
        .plus(watchRpc);

    assertThat(perCycle)
        .as("Single Ratis-client retry cycle worst-case wall-clock with "
                + "production defaults (writeRpc=%s, watchRpc=%s, maxRetries=%d, "
                + "maxBackoff=%s) must stay bounded; a regression here means "
                + "client writes against a dead pipeline can hang for minutes.",
            writeRpc, watchRpc, maxRetries, maxBackoff)
        .isLessThan(Duration.ofMinutes(4));
  }

  /**
   * Regression guard for HDDS-15444: the bounded exponential backoff is
   * what stops the Ratis client from retrying indefinitely. If this is
   * ever set back to {@code Integer.MAX_VALUE} (the pre-HDDS-15444
   * behaviour) write failures revert to multi-minute hangs.
   */
  @Test
  void defaultsCapExponentialMaxRetries() {
    RatisClientConfig subject = new OzoneConfiguration()
        .getObject(RatisClientConfig.class);

    assertThat(subject.getExponentialPolicyMaxRetries())
        .as("hdds.ratis.client.exponential.backoff.max.retries must remain "
            + "bounded; unbounded retries reintroduce the HDDS-15444 hang.")
        .isPositive()
        .isLessThanOrEqualTo(5);
  }

  /**
   * Regression guard for HDDS-15444: the client-side watch RPC timeout
   * must align with the server-side watch timeout (30 s by default).
   * If the client waits longer than the server is willing to honour, the
   * client hangs past the server-side abort.
   */
  @Test
  void defaultsAlignWatchTimeoutWithServer() {
    RatisClientConfig.RaftConfig raftSubject = new OzoneConfiguration()
        .getObject(RatisClientConfig.RaftConfig.class);

    assertThat(raftSubject.getRpcWatchRequestTimeout())
        .as("hdds.ratis.raft.client.rpc.watch.request.timeout should be "
            + "close to the server-side watch timeout (30 s); a much larger "
            + "value lets the client hang past the server's abort.")
        .isLessThanOrEqualTo(Duration.ofSeconds(60));
  }

}
