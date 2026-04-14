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
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for PendingContainerTracker.
 */
public class TestPendingContainerTracker {

  private static final long MAX_CONTAINER_SIZE = 5L * 1024 * 1024 * 1024; // 5GB

  private static final int NUM_DATANODES = 1000;
  private static final int NUM_PIPELINES = 1000;
  private static final int NUM_CONTAINERS = 10_000;
  private List<DatanodeDetails> datanodes;
  private List<Pipeline> pipelines;
  private List<ContainerID> containers;

  private PendingContainerTracker tracker;
  private Pipeline pipeline;
  private DatanodeDetails dn1;
  private DatanodeDetails dn2;

  /** First three container IDs. */
  private ContainerID container1;
  private ContainerID container2;

  @BeforeEach
  public void setUp() throws IOException {
    tracker = new PendingContainerTracker(MAX_CONTAINER_SIZE);

    datanodes = new ArrayList<>(NUM_DATANODES);
    for (int i = 0; i < NUM_DATANODES; i++) {
      datanodes.add(MockDatanodeDetails.randomLocalDatanodeDetails());
    }

    pipelines = new ArrayList<>(NUM_PIPELINES);
    for (int i = 0; i < NUM_PIPELINES; i++) {
      pipelines.add(MockPipeline.createPipeline(Collections.singletonList(datanodes.get(i))));
    }

    containers = new ArrayList<>(NUM_CONTAINERS);
    for (long id = 1; id <= NUM_CONTAINERS; id++) {
      containers.add(ContainerID.valueOf(id));
    }

    pipeline = MockPipeline.createPipeline(datanodes.subList(0, 3));
    dn1 = datanodes.get(0);
    dn2 = datanodes.get(1);

    container1 = containers.get(0);
    container2 = containers.get(1);
  }

  @Test
  public void testRecordPendingAllocation() {
    // Allocate first 100 containers across first 100 pipelines (1 DN each)
    for (int i = 0; i < 100; i++) {
      tracker.recordPendingAllocation(pipelines.get(i), containers.get(i));
    }

    // Each of the first 100 DNs should have 1 pending container
    for (int i = 0; i < 100; i++) {
      assertEquals(1, tracker.getPendingContainerCount(datanodes.get(i)));
      assertEquals(MAX_CONTAINER_SIZE, tracker.getPendingAllocationSize(datanodes.get(i)));
    }

    // DNs beyond index 100 should have 0 pending
    assertEquals(0, tracker.getPendingContainerCount(datanodes.get(500)));
    assertEquals(0, tracker.getPendingContainerCount(datanodes.get(999)));
  }

  @Test
  public void testRemovePendingAllocation() {
    // Allocate containers 0-99 to first 100 pipelines
    for (int i = 0; i < 100; i++) {
      tracker.recordPendingAllocation(pipelines.get(i), containers.get(i));
    }

    // Remove from first 50 DNs
    for (int i = 0; i < 50; i++) {
      tracker.removePendingAllocation(datanodes.get(i), containers.get(i));
    }

    // First 50 DNs should have 0 pending
    for (int i = 0; i < 50; i++) {
      assertEquals(0, tracker.getPendingContainerCount(datanodes.get(i)));
    }

    // DNs 50-99 should still have 1 pending
    for (int i = 50; i < 100; i++) {
      assertEquals(1, tracker.getPendingContainerCount(datanodes.get(i)));
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
    PendingContainerTracker shortRollTracker =
        new PendingContainerTracker(MAX_CONTAINER_SIZE, rollMs, null);

    shortRollTracker.recordPendingAllocationForDatanode(dn1, container1);
    assertEquals(1, shortRollTracker.getPendingContainerCount(dn1));
    assertTrue(shortRollTracker.containsPendingContainer(dn1, container1));

    // First roll: C1 moves from currentWindow to previousWindow; union still includes C1
    Thread.sleep(rollMs + 80);
    shortRollTracker.rollWindowsIfNeeded(dn1);
    assertEquals(1, shortRollTracker.getPendingContainerCount(dn1));
    assertTrue(shortRollTracker.containsPendingContainer(dn1, container1));

    // Second roll: prior previousWindow (holding C1) is dropped; C1 is no longer pending
    Thread.sleep(rollMs + 80);
    shortRollTracker.rollWindowsIfNeeded(dn1);
    assertEquals(0, shortRollTracker.getPendingContainerCount(dn1));
    assertEquals(0L, shortRollTracker.getPendingAllocationSize(dn1));
  }

  @Test
  public void testRemoveNonExistentContainer() {
    tracker.recordPendingAllocation(pipeline, container1);

    // Remove a container that was never added - should not throw exception
    tracker.removePendingAllocation(dn1, container2);

    // DN1 should still have container1
    assertEquals(1, tracker.getPendingContainerCount(dn1));
  }

  @Test
  public void testUnknownDatanodeHasZeroPendingCount() {
    DatanodeDetails unknownDN = MockDatanodeDetails.randomDatanodeDetails();
    assertEquals(0, tracker.getPendingContainerCount(unknownDN));
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
          tracker.recordPendingAllocation(pipeline, cid);

          if (j % 2 == 0) {
            tracker.removePendingAllocation(dn1, cid);
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
    tracker.recordPendingAllocation(pipeline, container1);

    assertEquals(1, tracker.getPendingContainerCount(dn1));

    // Remove the only pending container from DN1
    tracker.removePendingAllocation(dn1, container1);

    assertEquals(0, tracker.getPendingContainerCount(dn1));
    assertEquals(1, tracker.getPendingContainerCount(dn2));

    // Empty bucket for DN1 is still usable for new allocations
    tracker.recordPendingAllocationForDatanode(dn1, container2);
    assertEquals(1, tracker.getPendingContainerCount(dn1));
  }

  @Test
  public void testRemoveFromBothWindows() {
    // This test verifies that removal works from both current and previous windows
    // In general, a container could be in previous window after a roll

    // Add containers
    tracker.recordPendingAllocation(pipeline, container1);
    tracker.recordPendingAllocation(pipeline, container2);

    assertEquals(2, tracker.getPendingContainerCount(dn1));

    // Remove container1 - should work regardless of which window it's in
    tracker.removePendingAllocation(dn1, container1);

    assertEquals(1, tracker.getPendingContainerCount(dn1));

    assertFalse(tracker.containsPendingContainer(dn1, container1));
    assertTrue(tracker.containsPendingContainer(dn1, container2));
  }

  @Test
  public void testManyContainersOnSingleDatanode() {
    // Allocate first 1000 containers to the first datanode
    DatanodeDetails dn = datanodes.get(0);
    for (int i = 0; i < 1000; i++) {
      tracker.recordPendingAllocationForDatanode(dn, containers.get(i));
    }

    assertEquals(1000, tracker.getPendingContainerCount(dn));
    assertEquals(1000 * MAX_CONTAINER_SIZE, tracker.getPendingAllocationSize(dn));

    // Verify specific containers are present
    assertTrue(tracker.containsPendingContainer(dn, containers.get(0)));
    assertTrue(tracker.containsPendingContainer(dn, containers.get(500)));
    assertTrue(tracker.containsPendingContainer(dn, containers.get(999)));

    // Remove half of them
    for (int i = 0; i < 500; i++) {
      tracker.removePendingAllocation(dn, containers.get(i));
    }

    assertEquals(500, tracker.getPendingContainerCount(dn));
    assertFalse(tracker.containsPendingContainer(dn, containers.get(0)));
    assertTrue(tracker.containsPendingContainer(dn, containers.get(999)));
  }

  @Test
  public void testAllDatanodesWithMultipleContainers() {
    // Allocate 10 containers to each of the 1000 datanodes
    for (int dnIdx = 0; dnIdx < NUM_DATANODES; dnIdx++) {
      DatanodeDetails dn = datanodes.get(dnIdx);
      for (int cIdx = 0; cIdx < 10; cIdx++) {
        int containerIdx = dnIdx * 10 + cIdx;
        tracker.recordPendingAllocationForDatanode(dn, containers.get(containerIdx));
      }
    }

    // Each DN should have 10 pending containers
    for (int i = 0; i < NUM_DATANODES; i++) {
      assertEquals(10, tracker.getPendingContainerCount(datanodes.get(i)));
      assertEquals(10 * MAX_CONTAINER_SIZE, tracker.getPendingAllocationSize(datanodes.get(i)));
    }

    // Remove all containers from every 10th DN
    for (int dnIdx = 0; dnIdx < NUM_DATANODES; dnIdx += 10) {
      DatanodeDetails dn = datanodes.get(dnIdx);
      for (int cIdx = 0; cIdx < 10; cIdx++) {
        int containerIdx = dnIdx * 10 + cIdx;
        tracker.removePendingAllocation(dn, containers.get(containerIdx));
      }
    }

    // Every 10th DN should have 0 pending
    for (int i = 0; i < NUM_DATANODES; i += 10) {
      assertEquals(0, tracker.getPendingContainerCount(datanodes.get(i)));
    }

    // Other DNs should still have 10 pending
    assertEquals(10, tracker.getPendingContainerCount(datanodes.get(1)));
    assertEquals(10, tracker.getPendingContainerCount(datanodes.get(15)));
    assertEquals(10, tracker.getPendingContainerCount(datanodes.get(999)));
  }

  @Test
  public void testIdempotentRecording() {
    // Allocate same 100 containers multiple times to first 100 DNs
    DatanodeDetails dn = datanodes.get(0);

    for (int round = 0; round < 5; round++) {
      for (int i = 0; i < 100; i++) {
        tracker.recordPendingAllocationForDatanode(dn, containers.get(i));
      }
    }

    // Should still only have 100 containers
    assertEquals(100, tracker.getPendingContainerCount(dn));
  }
}
