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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.scm.node.NodeStatus.inServiceDead;
import static org.apache.hadoop.hdds.scm.node.NodeStatus.inServiceHealthy;
import static org.apache.hadoop.hdds.scm.node.NodeStatus.inServiceStale;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Comparator;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ECPipelineProvider#CREATE_FOR_READ_COMPARATOR}.
 */
class TestCreateForReadComparator {
  private final Comparator<NodeStatus> comparator = ECPipelineProvider.CREATE_FOR_READ_COMPARATOR;

  int compare(NodeStatus left, NodeStatus right) {
    return comparator.compare(left, right);
  }

  @Test
  void healthyFirst() {
    assertThat(0).isGreaterThan(compare(inServiceHealthy(), inServiceStale()));
    assertThat(0).isLessThan(compare(inServiceDead(), inServiceHealthy()));
    assertThat(0).isGreaterThan(compare(
        NodeStatus.valueOf(ENTERING_MAINTENANCE, HEALTHY),
        inServiceStale()
    ));
    assertThat(0).isLessThan(compare(
        inServiceStale(),
        NodeStatus.valueOf(DECOMMISSIONING, HEALTHY)
    ));
  }

  @Test
  void inServiceFirst() {
    assertThat(0).isGreaterThan(compare(
        inServiceHealthy(),
        NodeStatus.valueOf(ENTERING_MAINTENANCE, HEALTHY)));
    assertThat(0).isLessThan(compare(
        NodeStatus.valueOf(DECOMMISSIONING, HEALTHY),
        inServiceHealthy()
    ));
  }

}
