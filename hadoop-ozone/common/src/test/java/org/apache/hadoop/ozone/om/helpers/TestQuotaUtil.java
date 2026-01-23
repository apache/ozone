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

import static org.apache.hadoop.hdds.client.ECReplicationConfig.EcCodec.RS;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.junit.jupiter.api.Test;

/**
 * Tests for the QuotaUtil class.
 */
public class TestQuotaUtil {

  private static final int ONE_MB = 1024 * 1024;

  @Test
  public void testRatisThreeReplication() {
    ReplicationConfig repConfig = RatisReplicationConfig.getInstance(THREE);
    long replicatedSize =
        QuotaUtil.getReplicatedSize(123 * ONE_MB, repConfig);
    assertEquals(123 * ONE_MB * 3, replicatedSize);
  }

  @Test
  public void testRatisOneReplication() {
    ReplicationConfig repConfig = RatisReplicationConfig.getInstance(ONE);
    long replicatedSize =
        QuotaUtil.getReplicatedSize(123 * ONE_MB, repConfig);
    assertEquals(123 * ONE_MB, replicatedSize);
  }

  @Test
  public void testECFullStripeReplication() {
    ECReplicationConfig repConfig = new ECReplicationConfig(3, 2, RS, ONE_MB);
    long dataSize = ONE_MB * 3 * 123; // 123 full stripe
    long replicatedSize = QuotaUtil.getReplicatedSize(dataSize, repConfig);
    assertEquals(dataSize + 123 * ONE_MB * 2, replicatedSize);
  }

  @Test
  public void testECPartialStripeIntoFirstChunk() {
    ECReplicationConfig repConfig = new ECReplicationConfig(3, 2, RS, ONE_MB);
    long dataSize = ONE_MB * 3 * 123 + 10; // 123 full stripes, plus 10 bytes
    long replicatedSize = QuotaUtil.getReplicatedSize(dataSize, repConfig);
    // Expected is 123 parity stripes, plus another 10 bytes in each parity
    assertEquals(dataSize + 123 * ONE_MB * 2 + 10 * 2,
        replicatedSize);
  }

  @Test
  public void testECPartialStripeBeyondFirstChunk() {
    ECReplicationConfig repConfig = new ECReplicationConfig(3, 2, RS, ONE_MB);
    // 123 full stripes, plus 1MB+10 bytes
    long dataSize = ONE_MB * 3 * 123 + ONE_MB + 10;
    long replicatedSize = QuotaUtil.getReplicatedSize(dataSize, repConfig);
    // Expected is 123 parity stripes, plus another 1MB in each parity
    assertEquals(
        dataSize + 123 * ONE_MB * 2 + ONE_MB * 2, replicatedSize);
  }

  @Test
  public void testECPartialSingleStripeFirstChunk() {
    ECReplicationConfig repConfig = new ECReplicationConfig(3, 2, RS, ONE_MB);
    long dataSize = 10;
    long replicatedSize = QuotaUtil.getReplicatedSize(dataSize, repConfig);
    // Expected is 123 parity stripes, plus another 1MB in each parity
    assertEquals(dataSize + 10 * 2, replicatedSize);
  }

  @Test
  public void testECPartialSingleBeyondFirstChunk() {
    ECReplicationConfig repConfig = new ECReplicationConfig(3, 2, RS, ONE_MB);
    long dataSize = 2 * ONE_MB + 10;
    long replicatedSize = QuotaUtil.getReplicatedSize(dataSize, repConfig);
    // Expected is 123 parity stripes, plus another 1MB in each parity
    assertEquals(dataSize + ONE_MB * 2, replicatedSize);
  }

}
