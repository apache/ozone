/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the behavior of OzoneClientUtils APIs.
 */
public class TestOzoneClientUtils {
  private ReplicationConfig ecReplicationConfig =
      new ECReplicationConfig("rs-3-2-1024K");
  private ReplicationConfig ratis3ReplicationConfig =
      new RatisReplicationConfig(HddsProtos.ReplicationFactor.THREE);
  private ReplicationConfig ratis1ReplicationConfig =
      new RatisReplicationConfig(HddsProtos.ReplicationFactor.ONE);

  @Test
  public void testResolveClientSideRepConfigWhenBucketHasEC() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .resolveClientSideReplicationConfig(
            (short) 3, null,
            ecReplicationConfig, new OzoneConfiguration());
    // Bucket default is EC.
    Assert.assertEquals(ecReplicationConfig, replicationConfig);
  }

  /**
   * When bucket replication is null and it should respect fs passed value.
   */
  @Test
  public void testResolveClientSideRepConfigWhenBucketHasNull() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .resolveClientSideReplicationConfig(
            (short) 3, null, null,
            new OzoneConfiguration());
    // Passed replication is 3 - Ozone mapped replication is ratis THREE
    Assert.assertEquals(ratis3ReplicationConfig, replicationConfig);
  }

  /**
   * When bucket replication is null and it should return null if fs passed
   * value is invalid.
   */
  @Test
  public void testResolveClientSideRepConfigWhenFSPassedReplicationIsInvalid() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .resolveClientSideReplicationConfig(
            (short) -1, null, null,
            new OzoneConfiguration());
    // client configured value also null.
    // This API caller should leave the decision to server.
    Assert.assertNull(replicationConfig);
  }

  /**
   * When bucket default is non-EC and client side values are not valid, we
   * would just return null, so servers can make decision in this case.
   */
  @Test
  public void testResolveRepConfWhenFSPassedIsInvalidButBucketDefaultNonEC() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .resolveClientSideReplicationConfig(
            (short) -1, null, ratis3ReplicationConfig,
            new OzoneConfiguration());
    // Configured client config also null.
    Assert.assertNull(replicationConfig);
  }

  /**
   * When bucket default is non-EC and client side value is valid, we
   * would should return client side valid value.
   */
  @Test
  public void testResolveRepConfWhenFSPassedIsValidButBucketDefaultNonEC() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .resolveClientSideReplicationConfig(
            (short) 1, null, ratis3ReplicationConfig,
            new OzoneConfiguration());
    // Passed value is replication one - Ozone mapped value is ratis ONE
    Assert.assertEquals(ratis1ReplicationConfig, replicationConfig);
  }

  /**
   * When bucket default is EC and client side value also valid, we would just
   * return bucket default EC.
   */
  @Test
  public void testResolveRepConfWhenFSPassedIsValidButBucketDefaultEC() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .resolveClientSideReplicationConfig(
            (short) 3, ratis3ReplicationConfig,
            ecReplicationConfig, new OzoneConfiguration());
    // Bucket default is EC
    Assert.assertEquals(ecReplicationConfig, replicationConfig);
  }

  /**
   * When bucket default is non-EC and client side passed value also not valid
   * but configured value is valid, we would just return configured value.
   */
  @Test
  public void testResolveRepConfWhenFSPassedIsInvalidAndBucketDefaultNonEC() {
    ReplicationConfig replicationConfig = OzoneClientUtils
        .resolveClientSideReplicationConfig(
            (short) -1, ratis3ReplicationConfig, ratis1ReplicationConfig,
            new OzoneConfiguration());
    // Configured value is ratis THREE
    Assert.assertEquals(ratis3ReplicationConfig, replicationConfig);
  }
}
