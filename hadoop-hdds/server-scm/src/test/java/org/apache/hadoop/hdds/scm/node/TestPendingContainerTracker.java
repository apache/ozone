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

package org.apache.hadoop.hdds.scm.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for PendingContainerTracker.
 */
public class TestPendingContainerTracker {

  private static final long MAX_CONTAINER_SIZE = 5L * 1024 * 1024 * 1024; // 5GB
  private static final int NUM_DATANODES = 1000;
  private static final int NUM_CONTAINERS = 10_000;
  private List<DatanodeInfo> datanodes;
  private List<ContainerID> containers;

  private PendingContainerTracker tracker;
  private DatanodeInfo dn1;
  private DatanodeInfo dn2;

  /** First three container IDs. */
  private ContainerID container1;
  private ContainerID container2;

  @BeforeEach
  public void setUp() throws IOException {
    tracker = new PendingContainerTracker(MAX_CONTAINER_SIZE, HddsTestUtils.ROLL_INTERVAL_MS_DEFAULT, null);

    datanodes = new ArrayList<>(NUM_DATANODES);
    for (int i = 0; i < NUM_DATANODES; i++) {
      DatanodeInfo dn = new DatanodeInfo(
          MockDatanodeDetails.randomLocalDatanodeDetails(), NodeStatus.inServiceHealthy(), null,
          HddsTestUtils.ROLL_INTERVAL_MS_DEFAULT);
      setupDefaultStorageReport(dn);
      datanodes.add(dn);
    }

    containers = new ArrayList<>(NUM_CONTAINERS);
    for (long id = 1; id <= NUM_CONTAINERS; id++) {
      containers.add(ContainerID.valueOf(id));
    }

    dn1 = datanodes.get(0);
    dn2 = datanodes.get(1);

    container1 = containers.get(0);
    container2 = containers.get(1);
  }

  private void setupDefaultStorageReport(DatanodeInfo dn) {
    List<StorageReportProto> reports = new ArrayList<>();
    reports.add(createStorageReport(dn, 10_000 * MAX_CONTAINER_SIZE, 10_000 * MAX_CONTAINER_SIZE, 0));
    dn.updateStorageReports(reports);
  }

  @Test
  public void testRecordPendingAllocation() {
    // Allocate first 100 containers, one per datanode
    for (int i = 0; i < 100; i++) {
      tracker.checkSpaceAndRecordAllocation(datanodes.get(i), containers.get(i));
    }

    // Each of the first 100 DNs should have 1 pending container
    for (int i = 0; i < 100; i++) {
      assertEquals(1, datanodes.get(i).getPendingContainerAllocations().getCount());
      assertEquals(MAX_CONTAINER_SIZE,
          datanodes.get(i).getPendingContainerAllocations().getCount() * MAX_CONTAINER_SIZE);
    }

    // DNs beyond index 100 should have 0 pending
    assertEquals(0, datanodes.get(500).getPendingContainerAllocations().getCount());
    assertEquals(0, datanodes.get(999).getPendingContainerAllocations().getCount());
  }

  @Test
  public void testRemovePendingAllocation() {
    // Allocate containers 0-99, one per datanode
    for (int i = 0; i < 100; i++) {
      tracker.checkSpaceAndRecordAllocation(datanodes.get(i), containers.get(i));
    }

    // Remove from first 50 DNs
    for (int i = 0; i < 50; i++) {
      tracker.removePendingAllocation(datanodes.get(i).getPendingContainerAllocations(), containers.get(i));
    }

    // First 50 DNs should have 0 pending
    for (int i = 0; i < 50; i++) {
      assertEquals(0, datanodes.get(i).getPendingContainerAllocations().getCount());
    }

    // DNs 50-99 should still have 1 pending
    for (int i = 50; i < 100; i++) {
      assertEquals(1, datanodes.get(i).getPendingContainerAllocations().getCount());
    }
  }

  /**
   * After one roll interval, pending entries move from currentWindow to previousWindow and remain
   * visible. After a second roll (2× interval total), the old previousWindow is discarded and the
   * container ages out if not confirmed.
   */
  @Test
  @Timeout(30)
  public void testTwoWindowRollAgesOutContainerAfterTwoIntervals() throws InterruptedException {
    long rollMs = 200L;
    DatanodeInfo shortDn = new DatanodeInfo(
        MockDatanodeDetails.randomLocalDatanodeDetails(), NodeStatus.inServiceHealthy(), null,
        rollMs);

    PendingContainerTracker shortRollTracker = new PendingContainerTracker(MAX_CONTAINER_SIZE, rollMs, null);
    setupDefaultStorageReport(shortDn);

    shortRollTracker.checkSpaceAndRecordAllocation(shortDn, container1);
    assertEquals(1, shortDn.getPendingContainerAllocations().getCount());
    assertTrue(shortDn.getPendingContainerAllocations().contains(container1));

    // First roll: C1 moves from currentWindow to previousWindow; union still includes C1
    Thread.sleep(rollMs + 80);
    shortDn.getPendingContainerAllocations(); // triggers rollIfNeeded
    assertEquals(1, shortDn.getPendingContainerAllocations().getCount());
    assertTrue(shortDn.getPendingContainerAllocations().contains(container1));

    // Second roll: prior previousWindow (holding C1) is dropped; C1 is no longer pending
    Thread.sleep(rollMs + 80);
    shortDn.getPendingContainerAllocations(); // triggers rollIfNeeded
    assertEquals(0, shortDn.getPendingContainerAllocations().getCount());
    assertEquals(0L, shortDn.getPendingContainerAllocations().getCount() * MAX_CONTAINER_SIZE);
  }

  @Test
  public void testRemoveNonExistentContainer() {
    datanodes.subList(0, 3).forEach(dn -> tracker.checkSpaceAndRecordAllocation(dn, container1));

    // Remove a container that was never added - should not throw exception
    tracker.removePendingAllocation(dn1.getPendingContainerAllocations(), container2);

    // DN1 should still have container1
    assertEquals(1, dn1.getPendingContainerAllocations().getCount());
  }

  @Test
  public void testUnknownDatanodeHasZeroPendingCount() {
    DatanodeInfo unknownDN = new DatanodeInfo(
        MockDatanodeDetails.randomDatanodeDetails(), NodeStatus.inServiceHealthy(), null,
        HddsTestUtils.ROLL_INTERVAL_MS_DEFAULT);
    assertEquals(0, unknownDN.getPendingContainerAllocations().getCount());
  }

  @Test
  public void testConcurrentModification() throws InterruptedException {
    // Test thread-safety by having multiple threads add/remove containers
    final int numThreads = 10;
    final int operationsPerThread = 100;

    Thread[] threads = new Thread[numThreads];

    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      threads[i] = new Thread(() -> {
        for (int j = 0; j < operationsPerThread; j++) {
          ContainerID cid = ContainerID.valueOf(threadId * 1000L + j);
          datanodes.subList(0, 3).forEach(dn -> tracker.checkSpaceAndRecordAllocation(dn, cid));

          if (j % 2 == 0) {
            tracker.removePendingAllocation(dn1.getPendingContainerAllocations(), cid);
          }
        }
      });
    }

    // Start all threads
    for (Thread thread : threads) {
      thread.start();
    }

    // Wait for all to finish
    for (Thread thread : threads) {
      thread.join();
    }
  }

  @Test
  public void testBucketsRetainedWhenEmpty() {
    datanodes.subList(0, 3).forEach(dn -> tracker.checkSpaceAndRecordAllocation(dn, container1));

    assertEquals(1, dn1.getPendingContainerAllocations().getCount());

    // Remove the only pending container from DN1
    tracker.removePendingAllocation(dn1.getPendingContainerAllocations(), container1);

    assertEquals(0, dn1.getPendingContainerAllocations().getCount());
    assertEquals(1, dn2.getPendingContainerAllocations().getCount());

    // Empty bucket for DN1 is still usable for new allocations
    tracker.checkSpaceAndRecordAllocation(dn1, container2);
    assertEquals(1, dn1.getPendingContainerAllocations().getCount());
  }

  @Test
  public void testRemoveFromBothWindows() {
    // This test verifies that removal works from both current and previous windows
    // In general, a container could be in previous window after a roll

    // Add containers
    datanodes.subList(0, 3).forEach(dn -> tracker.checkSpaceAndRecordAllocation(dn, container1));
    datanodes.subList(0, 3).forEach(dn -> tracker.checkSpaceAndRecordAllocation(dn, container2));

    assertEquals(2, dn1.getPendingContainerAllocations().getCount());

    // Remove container1 - should work regardless of which window it's in
    tracker.removePendingAllocation(dn1.getPendingContainerAllocations(), container1);

    assertEquals(1, dn1.getPendingContainerAllocations().getCount());

    assertFalse(dn1.getPendingContainerAllocations().contains(container1));
    assertTrue(dn1.getPendingContainerAllocations().contains(container2));
  }

  @Test
  public void testManyContainersOnSingleDatanode() {
    // Allocate first 1000 containers to the first datanode
    DatanodeInfo dn = datanodes.get(0);
    for (int i = 0; i < 1000; i++) {
      tracker.checkSpaceAndRecordAllocation(dn, containers.get(i));
    }

    assertEquals(1000, dn.getPendingContainerAllocations().getCount());
    assertEquals(1000 * MAX_CONTAINER_SIZE, dn.getPendingContainerAllocations().getCount() * MAX_CONTAINER_SIZE);

    // Verify specific containers are present
    assertTrue(dn.getPendingContainerAllocations().contains(containers.get(0)));
    assertTrue(dn.getPendingContainerAllocations().contains(containers.get(500)));
    assertTrue(dn.getPendingContainerAllocations().contains(containers.get(999)));

    // Remove half of them
    for (int i = 0; i < 500; i++) {
      tracker.removePendingAllocation(dn.getPendingContainerAllocations(), containers.get(i));
    }

    assertEquals(500, dn.getPendingContainerAllocations().getCount());
    assertFalse(dn.getPendingContainerAllocations().contains(containers.get(0)));
    assertTrue(dn.getPendingContainerAllocations().contains(containers.get(999)));
  }

  @Test
  public void testAllDatanodesWithMultipleContainers() {
    // Allocate 10 containers to each of the 1000 datanodes
    for (int dnIdx = 0; dnIdx < NUM_DATANODES; dnIdx++) {
      DatanodeInfo dn = datanodes.get(dnIdx);
      for (int cIdx = 0; cIdx < 10; cIdx++) {
        int containerIdx = dnIdx * 10 + cIdx;
        tracker.checkSpaceAndRecordAllocation(dn, containers.get(containerIdx));
      }
    }

    // Each DN should have 10 pending containers
    for (int i = 0; i < NUM_DATANODES; i++) {
      assertEquals(10, datanodes.get(i).getPendingContainerAllocations().getCount());
      assertEquals(10 * MAX_CONTAINER_SIZE,
          datanodes.get(i).getPendingContainerAllocations().getCount() * MAX_CONTAINER_SIZE);
    }

    // Remove all containers from every 10th DN
    for (int dnIdx = 0; dnIdx < NUM_DATANODES; dnIdx += 10) {
      DatanodeInfo dn = datanodes.get(dnIdx);
      for (int cIdx = 0; cIdx < 10; cIdx++) {
        int containerIdx = dnIdx * 10 + cIdx;
        tracker.removePendingAllocation(dn.getPendingContainerAllocations(), containers.get(containerIdx));
      }
    }

    // Every 10th DN should have 0 pending
    for (int i = 0; i < NUM_DATANODES; i += 10) {
      assertEquals(0, datanodes.get(i).getPendingContainerAllocations().getCount());
    }

    // Other DNs should still have 10 pending
    assertEquals(10, datanodes.get(1).getPendingContainerAllocations().getCount());
    assertEquals(10, datanodes.get(15).getPendingContainerAllocations().getCount());
    assertEquals(10, datanodes.get(999).getPendingContainerAllocations().getCount());
  }

  @Test
  public void testIdempotentRecording() {
    // Allocate same 100 containers multiple times to first 100 DNs
    DatanodeInfo dn = datanodes.get(0);

    for (int round = 0; round < 5; round++) {
      for (int i = 0; i < 100; i++) {
        tracker.checkSpaceAndRecordAllocation(dn, containers.get(i));
      }
    }

    // Should still only have 100 containers
    assertEquals(100, dn.getPendingContainerAllocations().getCount());
  }

  @Test
  public void testMultiVolumeAccumulatedSpaceIsNotEnough() {
    long containerSize = MAX_CONTAINER_SIZE;

    DatanodeInfo dnInfo = datanodes.get(0);
    List<StorageReportProto> reports = new ArrayList<>();
    reports.add(createStorageReport(dnInfo, 100 * containerSize, containerSize / 4, 0));
    reports.add(createStorageReport(dnInfo, 100 * containerSize, containerSize / 4, 0));
    reports.add(createStorageReport(dnInfo, 100 * containerSize, containerSize / 2, 0));
    dnInfo.updateStorageReports(reports);

    assertFalse(tracker.checkSpaceAndRecordAllocation(dnInfo, containers.get(0)));
  }

  @Test
  public void testMultiVolumeWithPendingAllocation() {
    long containerSize = MAX_CONTAINER_SIZE;

    DatanodeInfo dnInfo = datanodes.get(0);

    // 3 volumes × 1 slot each = 3 total slots
    List<StorageReportProto> reports = new ArrayList<>();
    reports.add(createStorageReport(dnInfo, 100 * containerSize, containerSize, 0));
    reports.add(createStorageReport(dnInfo, 50 * containerSize, containerSize, 0));
    reports.add(createStorageReport(dnInfo, 100 * containerSize, containerSize, 0));
    dnInfo.updateStorageReports(reports);

    // Record 3 allocations atomically, each should succeed
    assertTrue(tracker.checkSpaceAndRecordAllocation(dnInfo, containers.get(0)));
    assertTrue(tracker.checkSpaceAndRecordAllocation(dnInfo, containers.get(1)));
    assertTrue(tracker.checkSpaceAndRecordAllocation(dnInfo, containers.get(2)));
    // All 3 slots consumed, 4th allocation must fail
    assertFalse(tracker.checkSpaceAndRecordAllocation(dnInfo, containers.get(3)));
  }

  @Test
  public void testMultiVolumeWithCommittedBytes() {
    long containerSize = MAX_CONTAINER_SIZE;

    DatanodeInfo dnInfo = datanodes.get(0);
    List<StorageReportProto> reports = new ArrayList<>();
    reports.add(createStorageReport(dnInfo, 100 * containerSize, 6 * containerSize, 5 * containerSize));
    reports.add(createStorageReport(dnInfo, 50 * containerSize, 3 * containerSize, 3 * containerSize));
    dnInfo.updateStorageReports(reports);

    // 1 slot available — first allocation succeeds and consumes it
    assertTrue(tracker.checkSpaceAndRecordAllocation(dnInfo, containers.get(0)));
    // 0 slots remaining
    assertFalse(tracker.checkSpaceAndRecordAllocation(dnInfo, containers.get(1)));
  }

  private StorageReportProto createStorageReport(DatanodeInfo dn, long capacity, long remaining, long committed) {
    return HddsTestUtils.createStorageReports(dn.getID(), capacity, remaining, committed).get(0);
  }
}
