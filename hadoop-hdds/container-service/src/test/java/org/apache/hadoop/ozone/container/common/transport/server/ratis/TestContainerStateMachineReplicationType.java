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

package org.apache.hadoop.ozone.container.common.transport.server.ratis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;

/**
 * Test {@link ContainerStateMachine#getReplicationType()}.
 */
class TestContainerStateMachineReplicationType {
  private static ContainerStateMachine.Builder newBuilder(ReplicationType type) {
    return ContainerStateMachine.newBuilder()
        .setReplicationType(type)
        .setGroupId(RaftGroupId.randomId())
        .setConf(new OzoneConfiguration())
        .setThreadNamePrefix("test");
  }

  @ParameterizedTest
  @EnumSource(value = ReplicationType.class, names = {"RATIS", "CHAINED"})
  void testSupportedReplicationType(ReplicationType type) {
    final ContainerStateMachine stateMachine = newBuilder(type).build();
    try {
      assertEquals(type, stateMachine.getReplicationType());
    } finally {
      stateMachine.close();
    }
  }

  @ParameterizedTest
  @EnumSource(value = ReplicationType.class, names = {"RATIS", "CHAINED"}, mode = EnumSource.Mode.EXCLUDE)
  @NullSource
  void testUnsupportedReplicationType(ReplicationType type) {
    assertThrows(IllegalArgumentException.class, () -> newBuilder(type).build());
  }
}
