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
package org.apache.hadoop.hdds.client;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test replicationConfig.
 */
public class TestReplicationConfig {

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

  @Test
  public void fromJavaObjects() {

    final ReplicationConfig replicationConfig = ReplicationConfig
        .fromTypeAndFactor(org.apache.hadoop.hdds.client.ReplicationType.RATIS,
            org.apache.hadoop.hdds.client.ReplicationFactor.THREE);

    Assert.assertEquals(replicationConfig.getReplicationType(),
        ReplicationType.RATIS);
    Assert.assertEquals(
        ((RatisReplicationConfig) replicationConfig).getReplicationFactor(),
        ReplicationFactor.THREE);

  }

  @Test
  public void fromTypeAndStringName() {

    ReplicationConfig replicationConfig = null;

    //RATIS-THREE
    replicationConfig = ReplicationConfig.fromTypeAndString(
        org.apache.hadoop.hdds.client.ReplicationType.RATIS, "THREE");

    Assert.assertEquals(replicationConfig.getReplicationType(),
        ReplicationType.RATIS);
    Assert.assertEquals(
        ((RatisReplicationConfig) replicationConfig).getReplicationFactor(),
        ReplicationFactor.THREE);

    //RATIS-ONE
    replicationConfig = ReplicationConfig.fromTypeAndString(
        org.apache.hadoop.hdds.client.ReplicationType.RATIS, "ONE");

    Assert.assertEquals(replicationConfig.getReplicationType(),
        ReplicationType.RATIS);
    Assert.assertEquals(
        ((RatisReplicationConfig) replicationConfig).getReplicationFactor(),
        ReplicationFactor.ONE);

    //STANDALONE-ONE
    replicationConfig = ReplicationConfig.fromTypeAndString(
        org.apache.hadoop.hdds.client.ReplicationType.STAND_ALONE, "ONE");

    Assert.assertEquals(replicationConfig.getReplicationType(),
        ReplicationType.STAND_ALONE);
    Assert.assertEquals(
        ((StandaloneReplicationConfig) replicationConfig)
            .getReplicationFactor(),
        ReplicationFactor.ONE);

  }


  @Test
  public void fromTypeAndStringInteger() {
    //RATIS-THREE
    ReplicationConfig replicationConfig = ReplicationConfig.fromTypeAndString(
        org.apache.hadoop.hdds.client.ReplicationType.RATIS, "3");

    Assert.assertEquals(replicationConfig.getReplicationType(),
        ReplicationType.RATIS);
    Assert.assertEquals(
        ((RatisReplicationConfig) replicationConfig).getReplicationFactor(),
        ReplicationFactor.THREE);
  }

  @Test
  public void adjustReplication() {
    ReplicationConfig config =
        new RatisReplicationConfig(ReplicationFactor.ONE);

    final ReplicationConfig replicationConfig =
        ReplicationConfig.adjustReplication(config, (short) 1);

    Assert.assertEquals(replicationConfig.getReplicationType(),
        ReplicationType.RATIS);
    Assert.assertEquals(
        ((RatisReplicationConfig) replicationConfig)
            .getReplicationFactor(),
        ReplicationFactor.ONE);

  }
}