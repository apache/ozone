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

package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.Test;

/**
 * Tests for the ContainerInfo class.
 */

public class TestContainerInfo {
  static int oldHash(long id) {
    return new HashCodeBuilder(61, 71)
        .append(id)
        .toHashCode();
  }

  static void assertHash(long value) {
    final ContainerID id = ContainerID.valueOf(value);
    assertEquals(oldHash(value), id.hashCode(), id::toString);
  }

  @Test
  void testContainIdHash() {
    for (int i = 0; i < 100; i++) {
      assertHash(i);
      final long id = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
      assertHash(id);
    }
  }

  @Test
  void getProtobufRatis() {
    ContainerInfo container = newBuilderForTest()
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .build();

    HddsProtos.ContainerInfoProto proto = container.getProtobuf();

    // No EC Config
    assertFalse(proto.hasEcReplicationConfig());
    assertEquals(THREE, proto.getReplicationFactor());
    assertEquals(RATIS, proto.getReplicationType());

    // Reconstruct object from Proto
    ContainerInfo recovered = ContainerInfo.fromProtobuf(proto);
    assertEquals(RATIS, recovered.getReplicationType());
    assertEquals(RatisReplicationConfig.class,
        recovered.getReplicationConfig().getClass());
    assertEquals(THREE, recovered.getReplicationFactor());
  }

  @Test
  void getProtobufEC() {
    // EC Config
    ContainerInfo container = newBuilderForTest()
        .setReplicationConfig(new ECReplicationConfig(3, 2))
        .build();

    HddsProtos.ContainerInfoProto proto = container.getProtobuf();

    assertEquals(3, proto.getEcReplicationConfig().getData());
    assertEquals(2, proto.getEcReplicationConfig().getParity());
    assertFalse(proto.hasReplicationFactor());
    assertEquals(EC, proto.getReplicationType());

    // Reconstruct object from Proto
    ContainerInfo recovered = ContainerInfo.fromProtobuf(proto);
    assertEquals(EC, recovered.getReplicationType());
    assertEquals(ECReplicationConfig.class,
        recovered.getReplicationConfig().getClass());
    ECReplicationConfig config =
        (ECReplicationConfig)recovered.getReplicationConfig();
    assertEquals(3, config.getData());
    assertEquals(2, config.getParity());
  }

  @Test
  void restoreState() {
    TestClock clock = TestClock.newInstance();
    ContainerInfo subject = newBuilderForTest()
        .setClock(clock)
        .build();

    final HddsProtos.LifeCycleState initialState = subject.getState();
    final Instant initialStateEnterTime = subject.getStateEnterTime();

    clock.fastForward(Duration.ofMinutes(1));
    subject.setState(CLOSING);

    assertEquals(CLOSING, subject.getState());
    assertEquals(clock.instant(), subject.getStateEnterTime());

    subject.revertState();
    assertEquals(initialState, subject.getState());
    assertEquals(initialStateEnterTime, subject.getStateEnterTime());

    assertThrows(IllegalStateException.class, subject::revertState);
  }

  public static ContainerInfo.Builder newBuilderForTest() {
    return new ContainerInfo.Builder()
        .setContainerID(1234)
        .setPipelineID(PipelineID.randomId())
        .setState(OPEN)
        .setOwner("scm");
  }
}
