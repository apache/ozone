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

import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.ADD;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.DELETE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for ContainerReplicaPendingOps.
 */
public class TestContainerReplicaPendingOps {

  private ContainerReplicaPendingOps pendingOps;
  private TestClock clock;
  private DatanodeDetails dn1;
  private DatanodeDetails dn2;
  private DatanodeDetails dn3;
  private ReplicationManagerMetrics metrics;
  private long deadline;
  private SCMCommand<?> addCmd;
  private SCMCommand<?> deleteCmd;

  @BeforeEach
  public void setup() {
    clock = new TestClock(Instant.now(), ZoneOffset.UTC);
    deadline = clock.millis() + 10000; // Current time plus 10 seconds
    pendingOps = new ContainerReplicaPendingOps(clock);

    ConfigurationSource conf = new OzoneConfiguration();
    ReplicationManager.ReplicationManagerConfiguration rmConf = conf
        .getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    ReplicationManager rm = mock(ReplicationManager.class);
    when(rm.getConfig()).thenReturn(rmConf);
    metrics = ReplicationManagerMetrics.create(rm);
    pendingOps.setReplicationMetrics(metrics);
    dn1 = MockDatanodeDetails.randomDatanodeDetails();
    dn2 = MockDatanodeDetails.randomDatanodeDetails();
    dn3 = MockDatanodeDetails.randomDatanodeDetails();

    addCmd = ReplicateContainerCommand.toTarget(1, dn3);
    deleteCmd =  new DeleteContainerCommand(1, false);
  }

  @AfterEach
  void cleanup() {
    if (metrics != null) {
      metrics.unRegister();
    }
  }

  @Test
  public void testGetPendingOpsReturnsEmptyList() {
    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(new ContainerID(1));
    assertEquals(0, ops.size());
  }

  @Test
  public void testClear() {
    pendingOps.scheduleAddReplica(new ContainerID(1), dn1, 0, addCmd, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(2), dn1, 0, deleteCmd, deadline);

    assertEquals(1, pendingOps.getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD));
    assertEquals(1, pendingOps.getPendingOpCount(ContainerReplicaOp.PendingOpType.DELETE));

    pendingOps.clear();

    assertEquals(0, pendingOps.getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD));
    assertEquals(0, pendingOps.getPendingOpCount(ContainerReplicaOp.PendingOpType.DELETE));
    assertEquals(0, pendingOps.getPendingOps(new ContainerID(1)).size());
    assertEquals(0, pendingOps.getPendingOps(new ContainerID(2)).size());

  }

  @Test
  public void testCanAddReplicasForAdd() {
    pendingOps.scheduleAddReplica(new ContainerID(1), dn1, 0, addCmd, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn2, 0, addCmd, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn3, 0, addCmd, deadline);
    // Duplicate for DN2
    pendingOps.scheduleAddReplica(new ContainerID(1), dn2, 0, addCmd, deadline + 1);
    // Not a duplicate for DN2 as different index. Should not happen in practice as it is not valid to have 2 indexes
    // on the same node.
    pendingOps.scheduleAddReplica(new ContainerID(1), dn2, 1, addCmd, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(2), dn1, 1, addCmd, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(2), dn1, 1, addCmd, deadline + 1);

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(new ContainerID(1));
    assertEquals(4, ops.size());
    for (ContainerReplicaOp op : ops) {
      if (!op.getTarget().equals(dn2)) {
        assertEquals(0, op.getReplicaIndex());
      }
      assertEquals(ADD, op.getOpType());
      if (op.getTarget().equals(dn2) && op.getReplicaIndex() == 0) {
        assertEquals(deadline + 1, op.getDeadlineEpochMillis());
      } else {
        assertEquals(deadline, op.getDeadlineEpochMillis());
      }
    }
    List<DatanodeDetails> allDns = ops.stream()
        .map(ContainerReplicaOp::getTarget).collect(Collectors.toList());
    assertThat(allDns).contains(dn1);
    assertThat(allDns).contains(dn2);
    assertThat(allDns).contains(dn3);

    ops = pendingOps.getPendingOps(new ContainerID(2));
    assertEquals(1, ops.size());
    assertEquals(1, ops.get(0).getReplicaIndex());
    assertEquals(ADD, ops.get(0).getOpType());
    assertEquals(dn1, ops.get(0).getTarget());
    assertEquals(deadline + 1, ops.get(0).getDeadlineEpochMillis());
  }

  @Test
  public void testCanAddReplicasForDelete() {
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn1, 0, deleteCmd, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn2, 0, deleteCmd, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn3, 0, deleteCmd, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(2), dn1, 1, deleteCmd, deadline);

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(new ContainerID(1));
    assertEquals(3, ops.size());
    for (ContainerReplicaOp op : ops) {
      assertEquals(0, op.getReplicaIndex());
      assertEquals(DELETE, op.getOpType());
    }
    List<DatanodeDetails> allDns = ops.stream()
        .map(ContainerReplicaOp::getTarget).collect(Collectors.toList());
    assertThat(allDns).contains(dn1);
    assertThat(allDns).contains(dn2);
    assertThat(allDns).contains(dn3);

    ops = pendingOps.getPendingOps(new ContainerID(2));
    assertEquals(1, ops.size());
    assertEquals(1, ops.get(0).getReplicaIndex());
    assertEquals(DELETE, ops.get(0).getOpType());
    assertEquals(dn1, ops.get(0).getTarget());
  }

  @Test
  public void testCompletingOps() {
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn1, 0, deleteCmd, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn1, 0, addCmd, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn2, 0, deleteCmd, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn3, 0, addCmd, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(2), dn1, 1, deleteCmd, deadline);

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(new ContainerID(1));

    // We expect 4 entries - 2 add and 2 delete.
    assertEquals(4, ops.size());

    assertTrue(pendingOps
        .completeAddReplica(new ContainerID(1), dn1, 0));
    ops = pendingOps.getPendingOps(new ContainerID(1));
    assertEquals(3, ops.size());

    // Complete one that does not exist:
    assertFalse(pendingOps
        .completeAddReplica(new ContainerID(1), dn1, 0));
    ops = pendingOps.getPendingOps(new ContainerID(1));
    assertEquals(3, ops.size());

    // Complete the remaining ones
    pendingOps.completeDeleteReplica(new ContainerID(1), dn1, 0);
    pendingOps.completeDeleteReplica(new ContainerID(1), dn2, 0);
    pendingOps.completeAddReplica(new ContainerID(1), dn3, 0);
    ops = pendingOps.getPendingOps(new ContainerID(1));
    assertEquals(0, ops.size());
  }

  @Test
  public void testRemoveSpecificOp() {
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn1, 0, deleteCmd, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn1, 0, addCmd, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn2, 0, deleteCmd, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn3, 0, addCmd, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(2), dn1, 1, deleteCmd, deadline);

    ContainerID cid = new ContainerID(1);
    List<ContainerReplicaOp> ops = pendingOps.getPendingOps(cid);
    assertEquals(4, ops.size());
    for (ContainerReplicaOp op : ops) {
      assertTrue(pendingOps.removeOp(cid, op));
    }
    // Attempt to remove one that no longer exists
    assertFalse(pendingOps.removeOp(cid, ops.get(0)));
    ops = pendingOps.getPendingOps(cid);
    assertEquals(0, ops.size());
  }

  @Test
  public void testRemoveExpiredEntries() {
    long expiry = clock.millis() + 1000;
    long laterExpiry =  clock.millis() + 2000;
    long latestExpiry = clock.millis() + 3000;
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn1, 0, deleteCmd, expiry);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn1, 0, addCmd, expiry);
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn2, 0, deleteCmd, laterExpiry);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn3, 0, addCmd, laterExpiry);
    pendingOps.scheduleDeleteReplica(new ContainerID(2), dn1, 1, deleteCmd, latestExpiry);
    pendingOps.scheduleAddReplica(new ContainerID(2), dn1, 1, addCmd, latestExpiry);

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(new ContainerID(1));
    assertEquals(4, ops.size());
    ops = pendingOps.getPendingOps(new ContainerID(2));
    assertEquals(2, ops.size());

    // Some entries expire at "start + 1000" some at start + 2000 and
    // start + 3000. Clock is currently at "start"
    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();
    // Nothing is remove as no deadline is older than the current clock time.
    ops = pendingOps.getPendingOps(new ContainerID(1));
    assertEquals(4, ops.size());

    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();
    // Those ADD with deadline + 1000 should be removed, but deletes are retained
    ops = pendingOps.getPendingOps(new ContainerID(1));
    assertEquals(3, ops.size());
    // We should lose the entries for DN1
    assertFalse(isOpPresent(ops, dn1, 0, ADD));
    assertTrue(isOpPresent(ops, dn1, 0, DELETE));
    assertTrue(isOpPresent(ops, dn2, 0, DELETE));

    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();

    // Now should only have entries for container 2 and the deletes for container 1
    ops = pendingOps.getPendingOps(new ContainerID(1));
    assertEquals(2, ops.size());

    assertTrue(isOpPresent(ops, dn1, 0, DELETE));
    assertTrue(isOpPresent(ops, dn2, 0, DELETE));

    ops = pendingOps.getPendingOps(new ContainerID(2));
    assertEquals(2, ops.size());

    // Advance the clock again and all should be removed except deletes
    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();
    ops = pendingOps.getPendingOps(new ContainerID(2));
    assertTrue(isOpPresent(ops, dn1, 1, DELETE));
    assertEquals(1, ops.size());
  }

  private boolean isOpPresent(List<ContainerReplicaOp> ops, DatanodeDetails dn,
      int index, ContainerReplicaOp.PendingOpType type) {
    return ops.stream().anyMatch(op -> op.getTarget().equals(dn) &&
        op.getReplicaIndex() == index && op.getOpType() == type);
  }

  @Test
  public void testReplicationMetrics() {
    long expiry = clock.millis() + 1000;
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn1, 1, deleteCmd, expiry);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn1, 2, addCmd, expiry);
    pendingOps.scheduleDeleteReplica(new ContainerID(2), dn2, 1, deleteCmd, expiry);
    pendingOps.scheduleAddReplica(new ContainerID(2), dn3, 1, addCmd, expiry);
    pendingOps.scheduleAddReplica(new ContainerID(3), dn3, 0, addCmd, expiry);
    pendingOps.scheduleDeleteReplica(new ContainerID(4), dn3, 0, deleteCmd, expiry);

    // InFlight Replication and Deletion
    assertEquals(3, pendingOps.getPendingOpCount(ADD));
    assertEquals(3, pendingOps.getPendingOpCount(DELETE));
    assertEquals(1, pendingOps.getPendingOpCount(ADD, ReplicationType.RATIS));
    assertEquals(1, pendingOps.getPendingOpCount(DELETE, ReplicationType.RATIS));
    assertEquals(2, pendingOps.getPendingOpCount(ADD, ReplicationType.EC));
    assertEquals(2, pendingOps.getPendingOpCount(DELETE, ReplicationType.EC));

    clock.fastForward(1500);

    pendingOps.removeExpiredEntries();

    // Two Delete and Replication command should be timeout
    assertEquals(metrics.getEcReplicaCreateTimeoutTotal(), 2);
    assertEquals(metrics.getEcReplicaDeleteTimeoutTotal(), 2);
    assertEquals(metrics.getReplicaCreateTimeoutTotal(), 1);
    assertEquals(metrics.getReplicaDeleteTimeoutTotal(), 1);

    expiry = clock.millis() + 1000;
    pendingOps.scheduleDeleteReplica(new ContainerID(3), dn1, 2, deleteCmd, expiry);
    pendingOps.scheduleAddReplica(new ContainerID(3), dn1, 3, addCmd, expiry);
    pendingOps.scheduleDeleteReplica(new ContainerID(4), dn2, 2, deleteCmd, expiry);
    pendingOps.scheduleAddReplica(new ContainerID(4), dn3, 4, addCmd, expiry);
    pendingOps.scheduleAddReplica(new ContainerID(5), dn3, 0, addCmd, expiry);
    pendingOps.scheduleDeleteReplica(new ContainerID(6), dn3, 0, deleteCmd, expiry);

    // InFlight Replication and Deletion. Previous Inflight should be
    // removed as they were timed out, but deletes are retained
    assertEquals(3, pendingOps.getPendingOpCount(ADD));
    assertEquals(6, pendingOps.getPendingOpCount(DELETE));

    pendingOps.completeDeleteReplica(new ContainerID(3), dn1, 2);
    pendingOps.completeAddReplica(new ContainerID(3), dn1, 3);
    pendingOps.completeDeleteReplica(new ContainerID(4), dn2, 2);
    pendingOps.completeAddReplica(new ContainerID(4), dn3, 4);
    pendingOps.completeDeleteReplica(new ContainerID(6), dn3, 0);
    pendingOps.completeAddReplica(new ContainerID(5), dn3, 0);

    assertEquals(metrics.getEcReplicasCreatedTotal(), 2);
    assertEquals(metrics.getEcReplicasDeletedTotal(), 2);
    assertEquals(metrics.getReplicasCreatedTotal(), 1);
    assertEquals(metrics.getReplicasDeletedTotal(), 1);

    pendingOps.completeDeleteReplica(new ContainerID(3), dn1, 2);
    pendingOps.completeAddReplica(new ContainerID(2), dn1, 3);

    // Checking pendingOpCount doesn't go below zero
    assertEquals(0, pendingOps.getPendingOpCount(ADD));
    assertEquals(3, pendingOps.getPendingOpCount(DELETE));
  }

  /**
   * Tests that registered subscribers are notified about completed and expired
   * ops.
   */
  @Test
  public void testNotifySubscribers() {
    // register subscribers
    ContainerReplicaPendingOpsSubscriber subscriber1 = mock(
        ContainerReplicaPendingOpsSubscriber.class);
    ContainerReplicaPendingOpsSubscriber subscriber2 = mock(
        ContainerReplicaPendingOpsSubscriber.class);
    pendingOps.registerSubscriber(subscriber1);
    pendingOps.registerSubscriber(subscriber2);

    // schedule an ADD and a DELETE
    ContainerID containerID = new ContainerID(1);
    pendingOps.scheduleAddReplica(containerID, dn1, 0, addCmd, deadline);
    ContainerReplicaOp addOp = pendingOps.getPendingOps(containerID).get(0);
    pendingOps.scheduleDeleteReplica(containerID, dn1, 0, deleteCmd, deadline);

    // complete the ADD and verify that subscribers were notified
    pendingOps.completeAddReplica(containerID, dn1, 0);
    verify(subscriber1, times(1)).opCompleted(addOp, containerID, false);
    verify(subscriber2, times(1)).opCompleted(addOp, containerID, false);

    // complete the DELETE and verify subscribers were notified
    ContainerReplicaOp deleteOp = pendingOps.getPendingOps(containerID).get(0);
    pendingOps.completeDeleteReplica(containerID, dn1, 0);
    verify(subscriber1, times(1)).opCompleted(deleteOp, containerID, false);
    verify(subscriber2, times(1)).opCompleted(deleteOp, containerID, false);

    // now, test notification on expiration
    pendingOps.scheduleDeleteReplica(containerID, dn1, 0, deleteCmd, deadline);
    pendingOps.scheduleAddReplica(containerID, dn2, 0, addCmd, deadline);
    for (ContainerReplicaOp op : pendingOps.getPendingOps(containerID)) {
      if (op.getOpType() == ADD) {
        addOp = op;
      } else {
        deleteOp = op;
      }
    }
    clock.fastForward(20000);
    pendingOps.removeExpiredEntries();
    // the clock is at 1000 and commands expired at 500
    verify(subscriber1, times(1)).opCompleted(addOp, containerID, true);
    verify(subscriber1, times(1)).opCompleted(deleteOp, containerID, true);
    verify(subscriber2, times(1)).opCompleted(addOp, containerID, true);
    verify(subscriber2, times(1)).opCompleted(deleteOp, containerID, true);
  }

  @Test
  public void subscribersShouldNotBeNotifiedWhenOpsHaveNotExpired() {
    ContainerID containerID = new ContainerID(1);

    // schedule ops
    pendingOps.scheduleDeleteReplica(containerID, dn1, 0, deleteCmd, deadline);
    pendingOps.scheduleAddReplica(containerID, dn2, 0, addCmd, deadline);

    // register subscriber
    ContainerReplicaPendingOpsSubscriber subscriber1 = mock(
        ContainerReplicaPendingOpsSubscriber.class);
    pendingOps.registerSubscriber(subscriber1);

    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();
    // no entries have expired, so there should be zero interactions with the
    // subscriber
    verifyNoMoreInteractions(subscriber1);
  }

  @Test
  public void subscribersShouldNotBeNotifiedWhenReplacingAnOpWithDuplicate() {
    ContainerID containerID = new ContainerID(1);

    // schedule ops
    pendingOps.scheduleAddReplica(containerID, dn2, 0, addCmd, deadline);

    // register subscriber
    ContainerReplicaPendingOpsSubscriber subscriber1 = mock(
        ContainerReplicaPendingOpsSubscriber.class);
    pendingOps.registerSubscriber(subscriber1);

    clock.fastForward(1000);
    pendingOps.scheduleAddReplica(containerID, dn2, 0, addCmd, deadline + 1);
    // no entries have expired, so there should be zero interactions with the
    // subscriber
    verifyNoMoreInteractions(subscriber1);
  }


}
