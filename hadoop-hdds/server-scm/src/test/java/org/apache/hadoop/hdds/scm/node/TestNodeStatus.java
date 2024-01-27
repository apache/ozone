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
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY_READONLY;
import static org.apache.hadoop.hdds.scm.node.NodeStatus.inServiceDead;
import static org.apache.hadoop.hdds.scm.node.NodeStatus.inServiceHealthy;
import static org.apache.hadoop.hdds.scm.node.NodeStatus.inServiceStale;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link NodeStatus}.
 */
class TestNodeStatus {

  @ParameterizedTest
  @EnumSource
  void readOnly(HddsProtos.NodeOperationalState state) {
    assertEquals(0, new NodeStatus(state, HEALTHY)
        .compareTo(new NodeStatus(state, HEALTHY_READONLY)));
  }

  @Test
  void healthyFirst() {
    assertThat(0).isGreaterThan(inServiceHealthy().compareTo(inServiceStale()));
    assertThat(0).isLessThan(inServiceDead().compareTo(inServiceHealthy()));
    assertThat(0).isGreaterThan(new NodeStatus(ENTERING_MAINTENANCE, HEALTHY).compareTo(
        inServiceStale()
    ));
    assertThat(0).isLessThan(inServiceStale().compareTo(
        new NodeStatus(DECOMMISSIONING, HEALTHY)
    ));
  }

  @Test
  void inServiceFirst() {
    assertThat(0).isGreaterThan(inServiceHealthy().compareTo(
        new NodeStatus(ENTERING_MAINTENANCE, HEALTHY)));
    assertThat(0).isLessThan(new NodeStatus(DECOMMISSIONING, HEALTHY).compareTo(
        inServiceHealthy()
    ));
  }

}
