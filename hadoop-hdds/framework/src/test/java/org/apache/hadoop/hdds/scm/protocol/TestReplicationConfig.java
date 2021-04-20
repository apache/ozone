/**
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
package org.apache.hadoop.hdds.scm.protocol;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test replicationConfig.
 */
public class TestReplicationConfig {

  @Test
  public void deserializeEC() {
    final ReplicationConfig replicationConfig = ReplicationConfig
        .fromProto(ReplicationType.EC, ReplicationFactor.THREE,
            HddsProtos.ECReplicationConfig.newBuilder()
                .setParity(2)
                .setData(3)
                .build());

    Assert
        .assertEquals(ECReplicationConfig.class, replicationConfig.getClass());

    ECReplicationConfig ecConfig = (ECReplicationConfig) replicationConfig;
    Assert.assertEquals(ReplicationType.EC, ecConfig.getReplicationType());
    Assert.assertEquals(3, ecConfig.getData());
    Assert.assertEquals(2, ecConfig.getParity());
  }

  @Test
  public void deserializeRatis() {
    final ReplicationConfig replicationConfig = ReplicationConfig
        .fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.THREE);

    Assert
        .assertEquals(RatisReplicationConfig.class,
            replicationConfig.getClass());

    RatisReplicationConfig ratisReplicationConfig =
        (RatisReplicationConfig) replicationConfig;
    Assert.assertEquals(ReplicationType.RATIS,
        ratisReplicationConfig.getReplicationType());
    Assert.assertEquals(ReplicationFactor.THREE,
        ratisReplicationConfig.getReplicationFactor());
  }

  @Test
  public void deserializeStandalone() {
    final ReplicationConfig replicationConfig = ReplicationConfig
        .fromTypeAndFactor(ReplicationType.STAND_ALONE, ReplicationFactor.ONE);

    Assert
        .assertEquals(StandaloneReplicationConfig.class,
            replicationConfig.getClass());

    StandaloneReplicationConfig standalone =
        (StandaloneReplicationConfig) replicationConfig;
    Assert.assertEquals(ReplicationType.STAND_ALONE,
        standalone.getReplicationType());
    Assert.assertEquals(ReplicationFactor.ONE,
        standalone.getReplicationFactor());
  }
}