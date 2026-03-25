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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.junit.jupiter.api.Test;

class TestContainerStateMap {

  @Test
  void testGetContainerIDs() {
    ContainerStateMap map = new ContainerStateMap();

    List<ContainerInfo> containerInfos = containerInfos();

    // initialize map
    containerInfos.forEach(map::addContainer);

    assertEquals(4, map.getContainerIDs(OPEN, ContainerID.MIN, containerInfos.size()).size());
    assertEquals(4, map.getContainerIDs(CLOSED, ContainerID.MIN, containerInfos.size()).size());

    // verify pagination
    assertEquals(3, map.getContainerIDs(CLOSED, ContainerID.MIN, 3).size());
    assertEquals(3, map.getContainerIDs(CLOSED, ContainerID.valueOf(7), 3).size());
  }

  private List<ContainerInfo> containerInfos() {
    return Arrays.asList(
        buildContainerInfo(1, OPEN),
        buildContainerInfo(2, CLOSED),
        buildContainerInfo(3, QUASI_CLOSED),
        buildContainerInfo(4, DELETED),
        buildContainerInfo(5, OPEN),
        buildContainerInfo(6, OPEN),
        buildContainerInfo(7, CLOSED),
        buildContainerInfo(8, CLOSED),
        buildContainerInfo(9, CLOSED),
        buildContainerInfo(10, OPEN)
    );
  }

  private ContainerInfo buildContainerInfo(long containerID, HddsProtos.LifeCycleState state) {
    return new ContainerInfo.Builder()
        .setContainerID(containerID)
        .setState(state)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(THREE))
        .build();
  }
}
