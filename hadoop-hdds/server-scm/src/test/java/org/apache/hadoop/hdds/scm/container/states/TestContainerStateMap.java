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

package org.apache.hadoop.hdds.scm.container.states;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.DELETED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.HEALTHY;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.MISSING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestContainerStateMap {

  private static ContainerStateMap map;

  @BeforeAll
  public static void setup() {
    map = new ContainerStateMap();
    containerInfos().forEach(map::addContainer);
  }

  @Test
  void testGetContainerIDs() {
    assertEquals(4, map.getContainerIDs(OPEN, null, ContainerID.MIN, 10).size());
    assertEquals(4, map.getContainerIDs(CLOSED, null, ContainerID.MIN, 10).size());

    // verify pagination
    assertEquals(3, map.getContainerIDs(CLOSED, null, ContainerID.MIN, 3).size());
    assertEquals(3, map.getContainerIDs(CLOSED, null, ContainerID.valueOf(7), 3).size());
  }

  /**
   * {@code getContainerIDs(lifecycle, health, start, count)} with {@code healthState == null}
   * uses the lifecycle index.
   */
  @Test
  void testGetContainerIDsForLifecycleState() {
    List<ContainerID> closed = map.getContainerIDs(CLOSED, null, ContainerID.MIN, 10);
    assertEquals(Arrays.asList(2L, 7L, 8L, 9L), toIds(closed));
  }

  /**
   * {@code getContainerIDs(lifecycle, health, start, count)} with both lifeCycleState and
   * healthState set uses the lifecycle index.
   */
  @Test
  void testGetContainerIDsWithLifeCycleStateAndHealthState() {
    List<ContainerID> missingClosed = map.getContainerIDs(CLOSED, MISSING, ContainerID.MIN, 10);
    assertEquals(Arrays.asList(2L, 8L), toIds(missingClosed));
  }

  /**
   * {@code getContainerIDs(lifecycle, health, start, count)} with {@code lifeCycleState == null}
   * scans the full container map (no lifecycle index).
   */
  @Test
  void testGetContainerIDsFullMapScanPath() {
    List<ContainerID> missing = map.getContainerIDs(null, MISSING, ContainerID.MIN, 10);
    assertEquals(Arrays.asList(1L, 2L, 4L, 8L), toIds(missing));
  }

  @Test
  void testPaginationAcrossPages() {
    List<ContainerID> page1 = map.getContainerIDs(CLOSED, null, ContainerID.MIN, 2);
    assertEquals(Arrays.asList(2L, 7L), toIds(page1));

    List<ContainerID> page2 = map.getContainerIDs(CLOSED, null, ContainerID.valueOf(8), 2);
    assertEquals(Arrays.asList(8L, 9L), toIds(page2));

    assertTrue(map.getContainerIDs(CLOSED, null, ContainerID.valueOf(10), 2).isEmpty());
  }

  @Test
  void testZeroCountReturnsEmptyList() {
    assertTrue(map.getContainerIDs(CLOSED, null, ContainerID.MIN, 0).isEmpty());
    assertTrue(map.getContainerIDs(null, MISSING, ContainerID.MIN, 0).isEmpty());
  }

  private static List<Long> toIds(List<ContainerID> ids) {
    return ids.stream().map(id -> id.getProtobuf().getId()).collect(Collectors.toList());
  }

  private static List<ContainerInfo> containerInfos() {
    return Arrays.asList(
        buildContainerInfo(1, OPEN, MISSING),
        buildContainerInfo(2, CLOSED, MISSING),
        buildContainerInfo(3, QUASI_CLOSED, HEALTHY),
        buildContainerInfo(4, DELETED, MISSING),
        buildContainerInfo(5, OPEN, HEALTHY),
        buildContainerInfo(6, OPEN, HEALTHY),
        buildContainerInfo(7, CLOSED, HEALTHY),
        buildContainerInfo(8, CLOSED, MISSING),
        buildContainerInfo(9, CLOSED, HEALTHY),
        buildContainerInfo(10, OPEN, HEALTHY)
    );
  }

  private static ContainerInfo buildContainerInfo(long containerID, HddsProtos.LifeCycleState state,
      ContainerHealthState healthState) {
    return new ContainerInfo.Builder()
        .setContainerID(containerID)
        .setState(state)
        .setHealthState(healthState)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(THREE))
        .build();
  }
}
