/**
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
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.ADD;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.DELETE;

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

  @BeforeEach
  public void setup() {
    clock = new TestClock(Instant.now(), ZoneOffset.UTC);
    deadline = clock.millis() + 10000; // Current time plus 10 seconds
    pendingOps = new ContainerReplicaPendingOps(clock);

    ConfigurationSource conf = new OzoneConfiguration();
    ReplicationManager.ReplicationManagerConfiguration rmConf = conf
        .getObject(ReplicationManager.ReplicationManagerConfiguration.class);
    ReplicationManager rm = Mockito.mock(ReplicationManager.class);
    Mockito.when(rm.getConfig()).thenReturn(rmConf);
    metrics = ReplicationManagerMetrics.create(rm);
    pendingOps.setReplicationMetrics(metrics);
    dn1 = MockDatanodeDetails.randomDatanodeDetails();
    dn2 = MockDatanodeDetails.randomDatanodeDetails();
    dn3 = MockDatanodeDetails.randomDatanodeDetails();
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
    Assertions.assertEquals(0, ops.size());
  }

  @Test
  public void testClear() {
    pendingOps.scheduleAddReplica(new ContainerID(1), dn1, 0, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(2), dn1, 0, deadline);

    Assertions.assertEquals(1,
        pendingOps.getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD));
    Assertions.assertEquals(1,
        pendingOps.getPendingOpCount(ContainerReplicaOp.PendingOpType.DELETE));

    pendingOps.clear();

    Assertions.assertEquals(0,
        pendingOps.getPendingOpCount(ContainerReplicaOp.PendingOpType.ADD));
    Assertions.assertEquals(0,
        pendingOps.getPendingOpCount(ContainerReplicaOp.PendingOpType.DELETE));
    Assertions.assertEquals(0,
        pendingOps.getPendingOps(new ContainerID(1)).size());
    Assertions.assertEquals(0,
        pendingOps.getPendingOps(new ContainerID(2)).size());

  }

  @Test
  public void testCanAddReplicasForAdd() {
    pendingOps.scheduleAddReplica(new ContainerID(1), dn1, 0, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn2, 0, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn3, 0, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(2), dn1, 1, deadline);

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(3, ops.size());
    for (ContainerReplicaOp op : ops) {
      Assertions.assertEquals(0, op.getReplicaIndex());
      Assertions.assertEquals(ADD, op.getOpType());
    }
    List<DatanodeDetails> allDns = ops.stream()
        .map(ContainerReplicaOp::getTarget).collect(Collectors.toList());
    Assertions.assertTrue(allDns.contains(dn1));
    Assertions.assertTrue(allDns.contains(dn2));
    Assertions.assertTrue(allDns.contains(dn3));

    ops = pendingOps.getPendingOps(new ContainerID(2));
    Assertions.assertEquals(1, ops.size());
    Assertions.assertEquals(1, ops.get(0).getReplicaIndex());
    Assertions.assertEquals(ADD, ops.get(0).getOpType());
    Assertions.assertEquals(dn1, ops.get(0).getTarget());
  }

  @Test
  public void testCanAddReplicasForDelete() {
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn1, 0, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn2, 0, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn3, 0, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(2), dn1, 1, deadline);

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(3, ops.size());
    for (ContainerReplicaOp op : ops) {
      Assertions.assertEquals(0, op.getReplicaIndex());
      Assertions.assertEquals(DELETE, op.getOpType());
    }
    List<DatanodeDetails> allDns = ops.stream()
        .map(ContainerReplicaOp::getTarget).collect(Collectors.toList());
    Assertions.assertTrue(allDns.contains(dn1));
    Assertions.assertTrue(allDns.contains(dn2));
    Assertions.assertTrue(allDns.contains(dn3));

    ops = pendingOps.getPendingOps(new ContainerID(2));
    Assertions.assertEquals(1, ops.size());
    Assertions.assertEquals(1, ops.get(0).getReplicaIndex());
    Assertions.assertEquals(DELETE, ops.get(0).getOpType());
    Assertions.assertEquals(dn1, ops.get(0).getTarget());
  }

  @Test
  public void testCompletingOps() {
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn1, 0, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn1, 0, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn2, 0, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn3, 0, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(2), dn1, 1, deadline);

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(new ContainerID(1));

    // We expect 4 entries - 2 add and 2 delete.
    Assertions.assertEquals(4, ops.size());

    Assertions.assertTrue(pendingOps
        .completeAddReplica(new ContainerID(1), dn1, 0));
    ops = pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(3, ops.size());

    // Complete one that does not exist:
    Assertions.assertFalse(pendingOps
        .completeAddReplica(new ContainerID(1), dn1, 0));
    ops = pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(3, ops.size());

    // Complete the remaining ones
    pendingOps.completeDeleteReplica(new ContainerID(1), dn1, 0);
    pendingOps.completeDeleteReplica(new ContainerID(1), dn2, 0);
    pendingOps.completeAddReplica(new ContainerID(1), dn3, 0);
    ops = pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(0, ops.size());
  }

  @Test
  public void testRemoveSpecificOp() {
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn1, 0, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn1, 0, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn2, 0, deadline);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn3, 0, deadline);
    pendingOps.scheduleDeleteReplica(new ContainerID(2), dn1, 1, deadline);

    ContainerID cid = new ContainerID(1);
    List<ContainerReplicaOp> ops = pendingOps.getPendingOps(cid);
    Assertions.assertEquals(4, ops.size());
    for (ContainerReplicaOp op : ops) {
      Assertions.assertTrue(pendingOps.removeOp(cid, op));
    }
    // Attempt to remove one that no longer exists
    Assertions.assertFalse(pendingOps.removeOp(cid, ops.get(0)));
    ops = pendingOps.getPendingOps(cid);
    Assertions.assertEquals(0, ops.size());
  }

  @Test
  public void testRemoveExpiredEntries() {
    long expiry = clock.millis() + 1000;
    long laterExpiry =  clock.millis() + 2000;
    long latestExpiry = clock.millis() + 3000;
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn1, 0, expiry);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn1, 0, expiry);
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn2, 0, laterExpiry);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn3, 0, laterExpiry);
    pendingOps.scheduleDeleteReplica(new ContainerID(2), dn1, 1, latestExpiry);

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(4, ops.size());
    ops = pendingOps.getPendingOps(new ContainerID(2));
    Assertions.assertEquals(1, ops.size());

    // Some entries expire at "start + 1000" some at start + 2000 and
    // start + 3000. Clock is currently at "start"
    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();
    // Nothing is remove as no deadline is older than the current clock time.
    ops = pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(4, ops.size());

    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();
    // Those with deadline + 1000 should be removed.
    ops = pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(2, ops.size());
    // We should lose the entries for DN1
    List<DatanodeDetails> dns = ops.stream()
        .map(ContainerReplicaOp::getTarget)
        .collect(Collectors.toList());
    Assertions.assertFalse(dns.contains(dn1));
    Assertions.assertTrue(dns.contains(dn2));
    Assertions.assertTrue(dns.contains(dn3));

    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();

    // Now should only have entries for container 2
    ops = pendingOps.getPendingOps(new ContainerID(1));
    Assertions.assertEquals(0, ops.size());
    ops = pendingOps.getPendingOps(new ContainerID(2));
    Assertions.assertEquals(1, ops.size());

    // Advance the clock again and all should be removed
    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();
    ops = pendingOps.getPendingOps(new ContainerID(2));
    Assertions.assertEquals(0, ops.size());
  }

  @Test
  public void testReplicationMetrics() {
    long expiry = clock.millis() + 1000;
    pendingOps.scheduleDeleteReplica(new ContainerID(1), dn1, 1, expiry);
    pendingOps.scheduleAddReplica(new ContainerID(1), dn1, 2, expiry);
    pendingOps.scheduleDeleteReplica(new ContainerID(2), dn2, 1, expiry);
    pendingOps.scheduleAddReplica(new ContainerID(2), dn3, 1, expiry);
    pendingOps.scheduleAddReplica(new ContainerID(3), dn3, 0, expiry);
    pendingOps.scheduleDeleteReplica(new ContainerID(4), dn3, 0, expiry);

    // InFlight Replication and Deletion
    Assertions.assertEquals(3, pendingOps.getPendingOpCount(ADD));
    Assertions.assertEquals(3, pendingOps.getPendingOpCount(DELETE));
    Assertions.assertEquals(1,
        pendingOps.getPendingOpCount(ADD, ReplicationType.RATIS));
    Assertions.assertEquals(1,
        pendingOps.getPendingOpCount(DELETE, ReplicationType.RATIS));
    Assertions.assertEquals(2,
        pendingOps.getPendingOpCount(ADD, ReplicationType.EC));
    Assertions.assertEquals(2,
        pendingOps.getPendingOpCount(DELETE, ReplicationType.EC));

    clock.fastForward(1500);

    pendingOps.removeExpiredEntries();

    // Two Delete and Replication command should be timeout
    Assertions.assertEquals(metrics.getEcReplicaCreateTimeoutTotal(), 2);
    Assertions.assertEquals(metrics.getEcReplicaDeleteTimeoutTotal(), 2);
    Assertions.assertEquals(metrics.getReplicaCreateTimeoutTotal(), 1);
    Assertions.assertEquals(metrics.getReplicaDeleteTimeoutTotal(), 1);

    expiry = clock.millis() + 1000;
    pendingOps.scheduleDeleteReplica(new ContainerID(3), dn1, 2, expiry);
    pendingOps.scheduleAddReplica(new ContainerID(3), dn1, 3, expiry);
    pendingOps.scheduleDeleteReplica(new ContainerID(4), dn2, 2, expiry);
    pendingOps.scheduleAddReplica(new ContainerID(4), dn3, 4, expiry);
    pendingOps.scheduleAddReplica(new ContainerID(5), dn3, 0, expiry);
    pendingOps.scheduleDeleteReplica(new ContainerID(6), dn3, 0, expiry);

    // InFlight Replication and Deletion. Previous Inflight should be
    // removed as they were timed out.
    Assertions.assertEquals(3, pendingOps.getPendingOpCount(ADD));
    Assertions.assertEquals(3, pendingOps.getPendingOpCount(DELETE));

    pendingOps.completeDeleteReplica(new ContainerID(3), dn1, 2);
    pendingOps.completeAddReplica(new ContainerID(3), dn1, 3);
    pendingOps.completeDeleteReplica(new ContainerID(4), dn2, 2);
    pendingOps.completeAddReplica(new ContainerID(4), dn3, 4);
    pendingOps.completeDeleteReplica(new ContainerID(6), dn3, 0);
    pendingOps.completeAddReplica(new ContainerID(5), dn3, 0);

    Assertions.assertEquals(metrics.getEcReplicasCreatedTotal(), 2);
    Assertions.assertEquals(metrics.getEcReplicasDeletedTotal(), 2);
    Assertions.assertEquals(metrics.getReplicasCreatedTotal(), 1);
    Assertions.assertEquals(metrics.getReplicasDeletedTotal(), 1);

    pendingOps.completeDeleteReplica(new ContainerID(3), dn1, 2);
    pendingOps.completeAddReplica(new ContainerID(2), dn1, 3);

    // Checking pendingOpCount doesn't go below zero
    Assertions.assertEquals(0, pendingOps.getPendingOpCount(ADD));
    Assertions.assertEquals(0, pendingOps.getPendingOpCount(DELETE));
  }

  /**
   * Tests that registered subscribers are notified about completed and expired
   * ops.
   */
  @Test
  public void testNotifySubscribers() {
    // register subscribers
    ContainerReplicaPendingOpsSubscriber subscriber1 = Mockito.mock(
        ContainerReplicaPendingOpsSubscriber.class);
    ContainerReplicaPendingOpsSubscriber subscriber2 = Mockito.mock(
        ContainerReplicaPendingOpsSubscriber.class);
    pendingOps.registerSubscriber(subscriber1);
    pendingOps.registerSubscriber(subscriber2);

    // schedule an ADD and a DELETE
    ContainerID containerID = new ContainerID(1);
    pendingOps.scheduleAddReplica(containerID, dn1, 0, deadline);
    ContainerReplicaOp addOp = pendingOps.getPendingOps(containerID).get(0);
    pendingOps.scheduleDeleteReplica(containerID, dn1, 0, deadline);

    // complete the ADD and verify that subscribers were notified
    pendingOps.completeAddReplica(containerID, dn1, 0);
    Mockito.verify(subscriber1, Mockito.times(1)).opCompleted(addOp,
        containerID, false);
    Mockito.verify(subscriber2, Mockito.times(1)).opCompleted(addOp,
        containerID, false);

    // complete the DELETE and verify subscribers were notified
    ContainerReplicaOp deleteOp = pendingOps.getPendingOps(containerID).get(0);
    pendingOps.completeDeleteReplica(containerID, dn1, 0);
    Mockito.verify(subscriber1, Mockito.times(1)).opCompleted(deleteOp,
        containerID, false);
    Mockito.verify(subscriber2, Mockito.times(1)).opCompleted(deleteOp,
        containerID, false);

    // now, test notification on expiration
    pendingOps.scheduleDeleteReplica(containerID, dn1, 0, deadline);
    pendingOps.scheduleAddReplica(containerID, dn2, 0, deadline);
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
    Mockito.verify(subscriber1, Mockito.times(1)).opCompleted(addOp,
        containerID, true);
    Mockito.verify(subscriber1, Mockito.times(1)).opCompleted(deleteOp,
        containerID, true);
    Mockito.verify(subscriber2, Mockito.times(1)).opCompleted(addOp,
        containerID, true);
    Mockito.verify(subscriber2, Mockito.times(1)).opCompleted(deleteOp,
        containerID, true);
  }

  @Test
  public void subscribersShouldNotBeNotifiedWhenOpsHaveNotExpired() {
    ContainerID containerID = new ContainerID(1);

    // schedule ops
    pendingOps.scheduleDeleteReplica(containerID, dn1, 0, deadline);
    pendingOps.scheduleAddReplica(containerID, dn2, 0, deadline);

    // register subscriber
    ContainerReplicaPendingOpsSubscriber subscriber1 = Mockito.mock(
        ContainerReplicaPendingOpsSubscriber.class);
    pendingOps.registerSubscriber(subscriber1);

    clock.fastForward(1000);
    pendingOps.removeExpiredEntries();
    // no entries have expired, so there should be zero interactions with the
    // subscriber
    Mockito.verifyZeroInteractions(subscriber1);
  }
}
