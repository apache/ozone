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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the server side replication config preference logic.
 */
public class TestOzoneConfigUtil {
  private ReplicationConfig ratis3ReplicationConfig =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
  private HddsProtos.ReplicationType noneType = HddsProtos.ReplicationType.NONE;
  private HddsProtos.ReplicationFactor zeroFactor =
      HddsProtos.ReplicationFactor.ZERO;
  private HddsProtos.ECReplicationConfig clientECReplicationConfig =
      new ECReplicationConfig("rs-3-2-1024K").toProto();
  private DefaultReplicationConfig bucketECConfig =
      new DefaultReplicationConfig(
          new ECReplicationConfig(clientECReplicationConfig));

  private OzoneManager ozoneManager;

  @BeforeEach
  public void setup() {
    ozoneManager = mock(OzoneManager.class);
    when(ozoneManager.getDefaultReplicationConfig())
        .thenReturn(ratis3ReplicationConfig);
  }

  /**
   * Tests EC bucket defaults.
   */
  @Test
  public void testResolveClientSideRepConfigWhenBucketHasEC() throws Exception {
    ReplicationConfig replicationConfig = OzoneConfigUtil
        .resolveReplicationConfigPreference(noneType, zeroFactor,
            clientECReplicationConfig, bucketECConfig, ozoneManager);
    // Client has no preference, so we should bucket defaults as we passed.
    assertEquals(bucketECConfig.getReplicationConfig(),
        replicationConfig);
  }

  /**
   * Tests server defaults.
   */
  @Test
  public void testResolveClientSideRepConfigWithNoClientAndBucketDefaults()
      throws Exception {
    ReplicationConfig replicationConfig = OzoneConfigUtil
        .resolveReplicationConfigPreference(noneType, zeroFactor,
            clientECReplicationConfig, null, ozoneManager);
    // Client has no preference, no bucket defaults, so it should return server
    // defaults.
    assertEquals(ratis3ReplicationConfig, replicationConfig);
  }

  /**
   * Tests client preference of EC.
   */
  @Test
  public void testResolveClientSideRepConfigWhenClientPassEC()
      throws Exception {
    ReplicationConfig replicationConfig = OzoneConfigUtil
        .resolveReplicationConfigPreference(HddsProtos.ReplicationType.EC,
            zeroFactor, clientECReplicationConfig, null,
            ozoneManager);
    // Client has preference of type EC, no bucket defaults, so it should return
    // client preference.
    assertEquals(new ECReplicationConfig("rs-3-2-1024K"),
        replicationConfig);
  }

  /**
   * Tests bucket ratis defaults.
   */
  @Test
  public void testResolveClientSideRepConfigWhenBucketHasEC3()
      throws Exception {
    ReplicationConfig ratisReplicationConfig =
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
    DefaultReplicationConfig ratisBucketDefaults =
        new DefaultReplicationConfig(ratisReplicationConfig);
    ReplicationConfig replicationConfig = OzoneConfigUtil
        .resolveReplicationConfigPreference(noneType, zeroFactor,
            clientECReplicationConfig, ratisBucketDefaults,
            ozoneManager);
    // Client has no preference of type and bucket has ratis defaults, so it
    // should return ratis.
    assertEquals(ratisReplicationConfig, replicationConfig);
  }
}
