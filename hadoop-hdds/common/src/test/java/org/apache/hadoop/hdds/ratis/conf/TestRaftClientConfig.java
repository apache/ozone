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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RatisClientConfig.RaftConfig}.
 */
class TestRaftClientConfig {

  @Test
  void defaults() {
    RatisClientConfig.RaftConfig subject = new RatisClientConfig.RaftConfig();
    RatisClientConfig.RaftConfig fromConfig =
        new OzoneConfiguration().getObject(RatisClientConfig.RaftConfig.class);

    assertEquals(fromConfig.getMaxOutstandingRequests(),
        subject.getMaxOutstandingRequests());
    assertEquals(fromConfig.getRpcRequestTimeout(),
        subject.getRpcRequestTimeout());
    assertEquals(fromConfig.getRpcWatchRequestTimeout(),
        subject.getRpcWatchRequestTimeout());
  }

  @Test
  void setAndGet() {
    RatisClientConfig.RaftConfig subject = new RatisClientConfig.RaftConfig();
    final int maxOutstandingRequests = 42;
    final Duration rpcRequestTimeout = Duration.ofMillis(12313);
    final Duration rpcWatchRequestTimeout = Duration.ofSeconds(99);

    subject.setMaxOutstandingRequests(maxOutstandingRequests);
    subject.setRpcRequestTimeout(rpcRequestTimeout);
    subject.setRpcWatchRequestTimeout(rpcWatchRequestTimeout);

    assertEquals(maxOutstandingRequests, subject.getMaxOutstandingRequests());
    assertEquals(rpcRequestTimeout, subject.getRpcRequestTimeout());
    assertEquals(rpcWatchRequestTimeout, subject.getRpcWatchRequestTimeout());
  }

}
