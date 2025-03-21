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

package org.apache.hadoop.ozone.container.diskbalancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask;
import org.apache.hadoop.ozone.container.replication.ContainerReplicator;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.hadoop.ozone.container.replication.ReplicationTask;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This is a test class for DefaultContainerChoosingPolicy.
 */
public class TestDefaultContainerChoosingPolicy {

  private OzoneContainer ozoneContainer;
  private ReplicationSupervisor replicationSupervisor;
  private DefaultContainerChoosingPolicy containerChoosingPolicy;
  private HddsVolume hddsVolume;
  private ContainerController containerController;
  private Collection<Container<?>> containers;
  private Set<AbstractReplicationTask> inFlight;
  private Set<Long> inProgressContainerIDs;

  private static final AtomicLong CONTAINER_SEQ_ID = new AtomicLong(100);
  private static final long CURRENT_TERM = 1;
  private final AtomicReference<ContainerReplicator> replicatorRef = new AtomicReference<>();

  @BeforeEach
  public void init() {
    ozoneContainer = mock(OzoneContainer.class);
    replicationSupervisor = mock(ReplicationSupervisor.class);
    containerChoosingPolicy = new DefaultContainerChoosingPolicy();
    inProgressContainerIDs = ConcurrentHashMap.newKeySet();
    hddsVolume = mock(HddsVolume.class);
    containerController = mock(ContainerController.class);
    when(ozoneContainer.getController()).thenReturn(containerController);
    containers = new ArrayList<>();
    inFlight = ConcurrentHashMap.newKeySet();
  }

  @Test
  public void testChooseContainer() {
    //container 103 -> inProgressContainerIds.
    inProgressContainerIDs.add(103L);

    // container 101, container 102 -> inFlight.
    inFlight.add(createTask(101L));
    inFlight.add(createTask(102L));

    for (int i = 0; i < 6; i++) {
      containers.add(createMockContainers());
    }

    when(replicationSupervisor.getInFlightTasks()).thenReturn(inFlight);
    when(containerController.getContainers(hddsVolume))
        .thenAnswer(i -> this.containers.iterator());

    ContainerData result = containerChoosingPolicy.chooseContainer(ozoneContainer, hddsVolume,
        inProgressContainerIDs, replicationSupervisor);

    assertEquals(106L, result.getContainerID(),
        "Only container 106 is choosen rest others are skipped");
  }

  private Container createMockContainers() {
    KeyValueContainer container = mock(KeyValueContainer.class);

    KeyValueContainerData data = mock(KeyValueContainerData.class);
    long containerId = CONTAINER_SEQ_ID.incrementAndGet();
    when(data.getContainerID()).thenReturn(containerId);

    //container 104 -> is empty and all others are non-empty
    if (containerId == 104L) {
      when(data.isEmpty()).thenReturn(true);
    } else {
      when(data.isEmpty()).thenReturn(false);
    }

    //container 105 -> is not closed and all others are closed
    if (containerId == 105L) {
      when(data.isClosed()).thenReturn(false);
    } else {
      when(data.isClosed()).thenReturn(true);
    }

    when(container.getContainerData()).thenReturn(data);
    return container;
  }

  private static ReplicateContainerCommand createCommand(long containerId) {
    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.forTest(containerId);
    cmd.setTerm(CURRENT_TERM);
    return cmd;
  }

  private ReplicationTask createTask(long containerId) {
    ReplicateContainerCommand cmd = createCommand(containerId);
    return new ReplicationTask(cmd, replicatorRef.get());
  }
}
