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

package org.apache.hadoop.hdds.scm.container.replication;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ContainerCheckRequest}.
 */
public class TestContainerCheckRequest {

  @Test
  public void testSetHealthStateUpdatesBothRequestAndReport() {
    ContainerInfo containerInfo = ReplicationTestUtil.createContainerInfo(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        1, CLOSED);
    ReplicationManagerReport report = new ReplicationManagerReport(10);

    ContainerCheckRequest request = new ContainerCheckRequest.Builder()
        .setContainerInfo(containerInfo)
        .setContainerReplicas(Collections.emptySet())
        .setPendingOps(Collections.emptyList())
        .setReport(report)
        .build();

    assertEquals(ContainerHealthState.HEALTHY, request.getHealthState());
    assertEquals(0, report.getStat(ContainerHealthState.UNDER_REPLICATED));

    request.setHealthState(ContainerHealthState.UNDER_REPLICATED);

    assertEquals(ContainerHealthState.UNDER_REPLICATED, request.getHealthState());
    assertEquals(1, report.getStat(ContainerHealthState.UNDER_REPLICATED));
  }
}
