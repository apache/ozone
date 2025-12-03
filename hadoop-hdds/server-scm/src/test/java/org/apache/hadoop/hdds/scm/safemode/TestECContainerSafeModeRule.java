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

package org.apache.hadoop.hdds.scm.safemode;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.server.events.EventQueue;

/**
 * This class tests ECContainerSafeModeRule.
 */
public class TestECContainerSafeModeRule extends AbstractContainerSafeModeRuleTest {
  @Override
  protected ReplicationType getReplicationType() {
    return ReplicationType.EC;
  }

  @Override
  protected AbstractContainerSafeModeRule createRule(
      EventQueue eventQueue,
      ConfigurationSource conf,
      ContainerManager containerManager,
      SCMSafeModeManager safeModeManager
  ) {
    return new ECContainerSafeModeRule(eventQueue, conf, containerManager, safeModeManager);
  }

  @Override
  protected ContainerInfo mockContainer(LifeCycleState state, long containerID) {
    ContainerInfo container = mock(ContainerInfo.class);
    when(container.getReplicationType()).thenReturn(ReplicationType.EC);
    when(container.getState()).thenReturn(state);
    when(container.getContainerID()).thenReturn(containerID);
    when(container.containerID()).thenReturn(ContainerID.valueOf(containerID));
    when(container.getNumberOfKeys()).thenReturn(1L);
    when(container.getReplicationConfig()).thenReturn(new ECReplicationConfig(3, 2));
    return container;
  }
}
