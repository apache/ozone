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

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.ozone.test.TestClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.ADD;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.DELETE;

/**
 * Tests for ContainerReplicaPendingOps.
 */
public class TestContainerReplicaPendingOps {

  private ContainerReplicaPendingOps pendingOps;
  private TestClock clock;
  private ConfigurationSource config;
  private DatanodeDetails dn1;
  private DatanodeDetails dn2;
  private DatanodeDetails dn3;

  @Before
  public void setup() {
    config = new OzoneConfiguration();
    clock = new TestClock(Instant.now(), ZoneOffset.UTC);
    pendingOps = new ContainerReplicaPendingOps(config, clock);
    dn1 = MockDatanodeDetails.randomDatanodeDetails();
    dn2 = MockDatanodeDetails.randomDatanodeDetails();
    dn3 = MockDatanodeDetails.randomDatanodeDetails();
  }

  @Test
  public void testGetPendingOpsReturnsEmptyList() {
    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(getContainerInfo(1));
    Assert.assertEquals(0, ops.size());
  }

  @Test
  public void testCanAddReplicasForAdd() {
    pendingOps.scheduleAddReplica(getContainerInfo(1), dn1, 0);
    pendingOps.scheduleAddReplica(getContainerInfo(1), dn2, 0);
    pendingOps.scheduleAddReplica(getContainerInfo(1), dn3, 0);
    pendingOps.scheduleAddReplica(getContainerInfo(2), dn1, 1);

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(getContainerInfo(1));
    Assert.assertEquals(3, ops.size());
    for (ContainerReplicaOp op : ops) {
      Assert.assertEquals(0, op.getReplicaIndex());
      Assert.assertEquals(ADD, op.getOpType());
    }
    List<DatanodeDetails> allDns = ops.stream()
        .map(s -> s.getTarget()).collect(Collectors.toList());
    Assert.assertTrue(allDns.contains(dn1));
    Assert.assertTrue(allDns.contains(dn2));
    Assert.assertTrue(allDns.contains(dn3));

    ops = pendingOps.getPendingOps(getContainerInfo(2));
    Assert.assertEquals(1, ops.size());
    Assert.assertEquals(1, ops.get(0).getReplicaIndex());
    Assert.assertEquals(ADD, ops.get(0).getOpType());
    Assert.assertEquals(dn1, ops.get(0).getTarget());
  }

  @Test
  public void testCanAddReplicasForDelete() {
    pendingOps.scheduleDeleteReplica(getContainerInfo(1), dn1, 0);
    pendingOps.scheduleDeleteReplica(getContainerInfo(1), dn2, 0);
    pendingOps.scheduleDeleteReplica(getContainerInfo(1), dn3, 0);
    pendingOps.scheduleDeleteReplica(getContainerInfo(2), dn1, 1);

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(getContainerInfo(1));
    Assert.assertEquals(3, ops.size());
    for (ContainerReplicaOp op : ops) {
      Assert.assertEquals(0, op.getReplicaIndex());
      Assert.assertEquals(DELETE, op.getOpType());
    }
    List<DatanodeDetails> allDns = ops.stream()
        .map(s -> s.getTarget()).collect(Collectors.toList());
    Assert.assertTrue(allDns.contains(dn1));
    Assert.assertTrue(allDns.contains(dn2));
    Assert.assertTrue(allDns.contains(dn3));

    ops = pendingOps.getPendingOps(getContainerInfo(2));
    Assert.assertEquals(1, ops.size());
    Assert.assertEquals(1, ops.get(0).getReplicaIndex());
    Assert.assertEquals(DELETE, ops.get(0).getOpType());
    Assert.assertEquals(dn1, ops.get(0).getTarget());
  }

  @Test
  public void testCompletingOps() {
    pendingOps.scheduleDeleteReplica(getContainerInfo(1), dn1, 0);
    pendingOps.scheduleAddReplica(getContainerInfo(1), dn1, 0);
    pendingOps.scheduleDeleteReplica(getContainerInfo(1), dn2, 0);
    pendingOps.scheduleAddReplica(getContainerInfo(1), dn3, 0);
    pendingOps.scheduleDeleteReplica(getContainerInfo(2), dn1, 1);

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(getContainerInfo(1));

    // We expect 4 entries - 2 add and 2 delete.
    Assert.assertEquals(4, ops.size());

    Assert.assertTrue(pendingOps
        .completeAddReplica(getContainerInfo(1), dn1, 0));
    ops = pendingOps.getPendingOps(getContainerInfo(1));
    Assert.assertEquals(3, ops.size());

    // Complete one that does not exist:
    Assert.assertFalse(pendingOps
        .completeAddReplica(getContainerInfo(1), dn1, 0));
    ops = pendingOps.getPendingOps(getContainerInfo(1));
    Assert.assertEquals(3, ops.size());

    // Complete the remaining ones
    pendingOps.completeDeleteReplica(getContainerInfo(1), dn1, 0);
    pendingOps.completeDeleteReplica(getContainerInfo(1), dn2, 0);
    pendingOps.completeAddReplica(getContainerInfo(1), dn3, 0);
    ops = pendingOps.getPendingOps(getContainerInfo(1));
    Assert.assertEquals(0, ops.size());
  }

  @Test
  public void testRemoveExpiredEntries() {
    pendingOps.scheduleDeleteReplica(getContainerInfo(1), dn1, 0);
    pendingOps.scheduleAddReplica(getContainerInfo(1), dn1, 0);
    clock.fastForward(1000);
    pendingOps.scheduleDeleteReplica(getContainerInfo(1), dn2, 0);
    pendingOps.scheduleAddReplica(getContainerInfo(1), dn3, 0);
    clock.fastForward(1000);
    pendingOps.scheduleDeleteReplica(getContainerInfo(2), dn1, 1);

    List<ContainerReplicaOp> ops =
        pendingOps.getPendingOps(getContainerInfo(1));
    Assert.assertEquals(4, ops.size());
    ops = pendingOps.getPendingOps(getContainerInfo(2));
    Assert.assertEquals(1, ops.size());

    // Some entries at "start" some at start + 1000 and start + 2000.
    // Clock is currently at +2000.
    pendingOps.removeExpiredEntries(2500);
    // Nothing is remove as nothing is older than the current clock time.
    ops = pendingOps.getPendingOps(getContainerInfo(1));
    Assert.assertEquals(4, ops.size());

    clock.fastForward(1000);
    pendingOps.removeExpiredEntries(2500);
    // Nothing is remove as nothing is older than the current clock time.
    ops = pendingOps.getPendingOps(getContainerInfo(1));
    Assert.assertEquals(2, ops.size());
    // We should lose the entries for DN1
    List<DatanodeDetails> dns = ops.stream()
        .map(s -> s.getTarget())
        .collect(Collectors.toList());
    Assert.assertFalse(dns.contains(dn1));
    Assert.assertTrue(dns.contains(dn2));
    Assert.assertTrue(dns.contains(dn3));

    clock.fastForward(1000);
    pendingOps.removeExpiredEntries(2500);

    // Now should only have entries for container 2
    ops = pendingOps.getPendingOps(getContainerInfo(1));
    Assert.assertEquals(0, ops.size());
    ops = pendingOps.getPendingOps(getContainerInfo(2));
    Assert.assertEquals(1, ops.size());

    // Advance the clock again and all should be removed
    clock.fastForward(1000);
    pendingOps.removeExpiredEntries(2500);
    ops = pendingOps.getPendingOps(getContainerInfo(2));
    Assert.assertEquals(0, ops.size());
  }


  private ContainerInfo getContainerInfo(long containerID) {
    return new ContainerInfo.Builder()
        .setContainerID(containerID)
        .setPipelineID(PipelineID.randomId())
        .setSequenceId(1)
        .setDeleteTransactionId(1)
        .setUsedBytes(1234)
        .setOwner("ozone")
        .setNumberOfKeys(10)
        .setState(HddsProtos.LifeCycleState.CLOSED)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .build();

  }

}
