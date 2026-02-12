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

package org.apache.hadoop.ozone.container.checksum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestReconcileContainerTask {
  private DNContainerOperationClient mockClient;
  private ContainerController mockController;

  @BeforeEach
  public void init() {
    mockClient = mock(DNContainerOperationClient.class);
    mockController = mock(ContainerController.class);
  }

  @Test
  public void testFailedTaskStatus() throws Exception {
    doThrow(IOException.class).when(mockController).reconcileContainer(any(), anyLong(), any());
    ReconcileContainerTask task = new ReconcileContainerTask(mockController, mockClient,
        new ReconcileContainerCommand(1, Collections.emptySet()));

    assertEquals(AbstractReplicationTask.Status.QUEUED, task.getStatus());
    task.runTask();
    assertEquals(AbstractReplicationTask.Status.FAILED, task.getStatus());
  }

  @Test
  public void testSuccessfulTaskStatus() {
    ReconcileContainerTask task = new ReconcileContainerTask(mockController, mockClient,
        new ReconcileContainerCommand(1, Collections.emptySet()));

    assertEquals(AbstractReplicationTask.Status.QUEUED, task.getStatus());
    task.runTask();
    assertEquals(AbstractReplicationTask.Status.DONE, task.getStatus());
  }

  @Test
  public void testEqualityWhenContainerIDsMatch() {
    final long containerID = 1;
    final UUID dnID1 = UUID.randomUUID();

    Set<DatanodeDetails> peerSet1 = new HashSet<>();
    peerSet1.add(buildDn(dnID1));
    Set<DatanodeDetails> peerSet1Other = new HashSet<>();
    peerSet1Other.add(buildDn(dnID1));
    Set<DatanodeDetails> peerSet2 = new HashSet<>();
    peerSet2.add(buildDn());

    ReconcileContainerTask peerSet1Task = new ReconcileContainerTask(mockController, mockClient,
        new ReconcileContainerCommand(containerID, peerSet1));
    ReconcileContainerTask otherPeerSet1Task = new ReconcileContainerTask(mockController, mockClient,
        new ReconcileContainerCommand(containerID, peerSet1Other));
    ReconcileContainerTask peerSet2Task = new ReconcileContainerTask(mockController, mockClient,
        new ReconcileContainerCommand(containerID, peerSet2));

    // Same container ID and peers.
    assertEquals(peerSet1Task, otherPeerSet1Task);
    // Same container ID, different peers - should still be equal now.
    assertEquals(peerSet1Task, peerSet2Task);
  }

  @Test
  public void testEqualityWhenContainerIDsDifferent() {
    Set<DatanodeDetails> peerSet = new HashSet<>();
    peerSet.add(buildDn());

    ReconcileContainerTask id1Task = new ReconcileContainerTask(mockController, mockClient,
        new ReconcileContainerCommand(1, peerSet));
    ReconcileContainerTask id2Task = new ReconcileContainerTask(mockController, mockClient,
        new ReconcileContainerCommand(2, peerSet));
    ReconcileContainerTask id2NoPeersTask = new ReconcileContainerTask(mockController, mockClient,
        new ReconcileContainerCommand(2, Collections.emptySet()));

    // Different container ID, same peers.
    assertNotEquals(id1Task, id2Task);
    // Different container ID, different peers.
    assertNotEquals(id1Task, id2NoPeersTask);
  }

  private DatanodeDetails buildDn(UUID id) {
    return DatanodeDetails.newBuilder()
        .setUuid(id)
        .build();
  }

  private DatanodeDetails buildDn() {
    return DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .build();
  }
}
