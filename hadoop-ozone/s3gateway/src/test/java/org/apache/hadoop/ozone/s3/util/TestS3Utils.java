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
package org.apache.hadoop.ozone.s3.util;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests the S3Utils APIs.
 */
public class TestS3Utils {
  private ReplicationConfig ecReplicationConfig =
      new ECReplicationConfig("rs-3-2-1024K");
  private ReplicationConfig ratis3ReplicationConfig =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
  private ReplicationConfig ratis1ReplicationConfig =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE);

  @Test
  public void testResolveClientSideRepConfigWhenBucketHasEC()
      throws OS3Exception {
    ReplicationConfig replicationConfig = S3Utils
        .resolveS3ClientSideReplicationConfig(S3StorageType.STANDARD.name(),
            null, ecReplicationConfig);
    // Bucket default is EC.
    assertEquals(ecReplicationConfig, replicationConfig);
  }

  /**
   * When bucket replication is null and it should respect user passed value.
   */
  @Test
  public void testResolveClientSideRepConfigWhenBucketHasNull()
      throws OS3Exception {
    ReplicationConfig replicationConfig = S3Utils
        .resolveS3ClientSideReplicationConfig(S3StorageType.STANDARD.name(),
            null, null);
    // Passed replication is 3 - Ozone mapped replication is ratis THREE
    assertEquals(ratis3ReplicationConfig, replicationConfig);
  }

  /**
   * When bucket replication is null and it should return null if user passed
   * value is invalid.
   */
  @Test
  public void testResolveClientSideRepConfigWhenUserPassedReplicationIsEmpty()
      throws OS3Exception {
    ReplicationConfig replicationConfig =
        S3Utils.resolveS3ClientSideReplicationConfig("", null, null);
    // client configured value also null.
    // This API caller should leave the decision to server.
    assertNull(replicationConfig);
  }

  /**
   * When bucket default is non-EC and client side values are not valid, we
   * would just return null, so servers can make decision in this case.
   */
  @Test
  public void testResolveRepConfWhenUserPassedIsInvalidButBucketDefaultNonEC()
      throws OS3Exception {
    ReplicationConfig replicationConfig = S3Utils
        .resolveS3ClientSideReplicationConfig(null, null,
            ratis3ReplicationConfig);
    // Configured client config also null.
    assertNull(replicationConfig);
  }

  /**
   * When bucket default is non-EC and client side value is valid, we
   * would should return client side valid value.
   */
  @Test
  public void testResolveRepConfWhenUserPassedIsValidButBucketDefaultNonEC()
      throws OS3Exception {
    ReplicationConfig replicationConfig = S3Utils
        .resolveS3ClientSideReplicationConfig(
            S3StorageType.REDUCED_REDUNDANCY.name(), null,
            ratis3ReplicationConfig);
    // Passed value is replication one - Ozone mapped value is ratis ONE
    assertEquals(ratis1ReplicationConfig, replicationConfig);
  }

  /**
   * When bucket default is EC and client side value also valid, we would just
   * return bucket default EC.
   */
  @Test
  public void testResolveRepConfWhenUserPassedIsValidButBucketDefaultEC()
      throws OS3Exception {
    ReplicationConfig replicationConfig = S3Utils
        .resolveS3ClientSideReplicationConfig(S3StorageType.STANDARD.name(),
            ratis3ReplicationConfig, ecReplicationConfig);
    // Bucket default is EC
    assertEquals(ecReplicationConfig, replicationConfig);
  }

  /**
   * When bucket default is non-EC and client side passed value also not valid
   * but configured value is valid, we would just return configured value.
   */
  @Test
  public void testResolveRepConfWhenUserPassedIsInvalidAndBucketDefaultNonEC()
      throws OS3Exception {
    ReplicationConfig replicationConfig = S3Utils
        .resolveS3ClientSideReplicationConfig(null, ratis3ReplicationConfig,
            ratis1ReplicationConfig);
    // Configured value is ratis THREE
    assertEquals(ratis3ReplicationConfig, replicationConfig);
  }

  /**
   * When bucket default is non-EC and client side passed value also not valid
   * but configured value is valid, we would just return configured value.
   */
  @Test
  public void testResolveRepConfWhenUserPassedIsInvalid() throws OS3Exception {
    assertThrows(OS3Exception.class, () -> S3Utils.
        resolveS3ClientSideReplicationConfig(
            "INVALID", ratis3ReplicationConfig, ratis1ReplicationConfig));
  }

}
