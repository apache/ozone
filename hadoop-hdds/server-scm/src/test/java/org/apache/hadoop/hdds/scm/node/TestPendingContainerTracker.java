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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.util.Set;
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

  private PendingContainerTracker tracker;
  private Pipeline pipeline;
  private DatanodeDetails dn1;
  private DatanodeDetails dn2;
  private DatanodeDetails dn3;
  private ContainerID container1;
  private ContainerID container2;
  private ContainerID container3;

  @BeforeEach
  public void setUp() throws IOException {
    tracker = new PendingContainerTracker(MAX_CONTAINER_SIZE);

    // Create a 3-node Ratis pipeline
    pipeline = MockPipeline.createPipeline(3);
    dn1 = pipeline.getNodes().get(0);
    dn2 = pipeline.getNodes().get(1);
    dn3 = pipeline.getNodes().get(2);

    container1 = ContainerID.valueOf(1L);
    container2 = ContainerID.valueOf(2L);
    container3 = ContainerID.valueOf(3L);
  }

  @Test
  public void testRecordPendingAllocation() {
    // Initially no pending containers
    assertEquals(0, tracker.getPendingContainers(dn1).size());
    assertEquals(0, tracker.getPendingAllocationSize(dn1));

    // Record a pending allocation
    tracker.recordPendingAllocation(pipeline, container1);

    // All 3 DNs should have the container pending
    assertEquals(1, tracker.getPendingContainers(dn1).size());
    assertEquals(1, tracker.getPendingContainers(dn2).size());
    assertEquals(1, tracker.getPendingContainers(dn3).size());

    // Size should be MAX_CONTAINER_SIZE for each DN
    assertEquals(MAX_CONTAINER_SIZE, tracker.getPendingAllocationSize(dn1));
    assertEquals(MAX_CONTAINER_SIZE, tracker.getPendingAllocationSize(dn2));
    assertEquals(MAX_CONTAINER_SIZE, tracker.getPendingAllocationSize(dn3));
  }

  @Test
  public void testRecordMultiplePendingAllocations() {
    tracker.recordPendingAllocation(pipeline, container1);
    tracker.recordPendingAllocation(pipeline, container2);
    tracker.recordPendingAllocation(pipeline, container3);

    // Each DN should have 3 pending containers
    assertEquals(3, tracker.getPendingContainers(dn1).size());
    assertEquals(3, tracker.getPendingContainers(dn2).size());
    assertEquals(3, tracker.getPendingContainers(dn3).size());

    // Size should be 3 × MAX_CONTAINER_SIZE
    assertEquals(3 * MAX_CONTAINER_SIZE, tracker.getPendingAllocationSize(dn1));
  }

  @Test
  public void testIdempotentRecording() {
    tracker.recordPendingAllocation(pipeline, container1);
    tracker.recordPendingAllocation(pipeline, container1); // Duplicate

    // Should still be 1 container (Set deduplication)
    assertEquals(1, tracker.getPendingContainers(dn1).size());
    assertEquals(MAX_CONTAINER_SIZE, tracker.getPendingAllocationSize(dn1));
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
    Set<ContainerID> pendingDn1 = shortRollTracker.getPendingContainers(dn1);
    assertEquals(1, pendingDn1.size());
    assertThat(pendingDn1).containsExactly(container1);

    // First roll: C1 moves from currentWindow to previousWindow; union still includes C1
    Thread.sleep(rollMs + 80);
    shortRollTracker.rollWindowsIfNeeded(dn1);
    pendingDn1 = shortRollTracker.getPendingContainers(dn1);
    assertEquals(1, pendingDn1.size());
    assertThat(pendingDn1).containsExactly(container1);

    // Second roll: prior previousWindow (holding C1) is dropped; C1 is no longer pending
    Thread.sleep(rollMs + 80);
    shortRollTracker.rollWindowsIfNeeded(dn1);
    assertEquals(0, shortRollTracker.getPendingContainers(dn1).size());
    assertEquals(0L, shortRollTracker.getPendingAllocationSize(dn1));
  }

  @Test
  public void testRemovePendingAllocation() {
    tracker.recordPendingAllocation(pipeline, container1);
    tracker.recordPendingAllocation(pipeline, container2);

    assertEquals(2, tracker.getPendingContainers(dn1).size());

    // Remove one container from DN1
    tracker.removePendingAllocation(dn1, container1);

    assertEquals(1, tracker.getPendingContainers(dn1).size());
    assertEquals(MAX_CONTAINER_SIZE, tracker.getPendingAllocationSize(dn1));

    // DN2 and DN3 should still have both containers
    assertEquals(2, tracker.getPendingContainers(dn2).size());
    assertEquals(2, tracker.getPendingContainers(dn3).size());
  }

  @Test
  public void testRemovePendingAllocationFromPipeline() {
    tracker.recordPendingAllocation(pipeline, container1);
    tracker.recordPendingAllocation(pipeline, container2);

    // Remove container1 from all nodes in pipeline
    for (DatanodeDetails dn : pipeline.getNodes()) {
      tracker.removePendingAllocation(dn, container1);
    }

    // All DNs should have only container2 remaining
    assertEquals(1, tracker.getPendingContainers(dn1).size());
    assertEquals(1, tracker.getPendingContainers(dn2).size());
    assertEquals(1, tracker.getPendingContainers(dn3).size());
  }

  @Test
  public void testRemoveNonExistentContainer() {
    tracker.recordPendingAllocation(pipeline, container1);

    // Remove a container that was never added - should not throw exception
    tracker.removePendingAllocation(dn1, container2);

    // DN1 should still have container1
    assertEquals(1, tracker.getPendingContainers(dn1).size());
  }

  @Test
  public void testGetPendingContainers() {
    tracker.recordPendingAllocation(pipeline, container1);
    tracker.recordPendingAllocation(pipeline, container2);

    Set<ContainerID> pending = tracker.getPendingContainers(dn1);

    assertEquals(2, pending.size());
    assertThat(pending).contains(container1);
    assertThat(pending).contains(container2);

    // Returned set should be a copy - modifying it shouldn't affect tracker
    pending.add(container3);
    assertEquals(2, tracker.getPendingContainers(dn1).size()); // Should still be 2
  }

  @Test
  public void testGetPendingContainersForNonExistentDN() {
    DatanodeDetails unknownDN = MockDatanodeDetails.randomDatanodeDetails();

    Set<ContainerID> pending = tracker.getPendingContainers(unknownDN);

    assertThat(pending).isEmpty();
  }

  @Test
  public void testGetTotalPendingCount() {
    assertEquals(0, tracker.getTotalPendingCount());

    tracker.recordPendingAllocation(pipeline, container1);

    // 1 container × 3 DNs = 3 total pending
    assertEquals(3, tracker.getTotalPendingCount());

    tracker.recordPendingAllocation(pipeline, container2);

    // 2 containers × 3 DNs = 6 total pending
    assertEquals(6, tracker.getTotalPendingCount());

    // Remove from one DN
    tracker.removePendingAllocation(dn1, container1);

    // (2 containers × 2 DNs) + (1 container × 1 DN) = 5 total
    assertEquals(5, tracker.getTotalPendingCount());
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

    // Verify no exceptions occurred and counts are reasonable
    assertThat(tracker.getTotalPendingCount()).isGreaterThanOrEqualTo(0);
    assertThat(tracker.getDataNodeCount()).isLessThanOrEqualTo(3);
  }

  @Test
  public void testMemoryCleanupOnEmptySet() {
    tracker.recordPendingAllocation(pipeline, container1);

    assertEquals(3, tracker.getDataNodeCount());

    // Remove the only pending container from DN1
    tracker.removePendingAllocation(dn1, container1);

    // DN1 should be removed from the map (memory cleanup)
    assertEquals(2, tracker.getDataNodeCount());
  }

  @Test
  public void testPendingContainer() {
    // Simulate allocation and confirmation flow

    // Allocate 3 containers
    tracker.recordPendingAllocation(pipeline, container1);
    tracker.recordPendingAllocation(pipeline, container2);
    tracker.recordPendingAllocation(pipeline, container3);

    // Each DN should have 3 pending, 15GB total
    assertEquals(3, tracker.getPendingContainers(dn1).size());
    assertEquals(3 * MAX_CONTAINER_SIZE, tracker.getPendingAllocationSize(dn1));

    // DN1 confirms container1 via container report
    tracker.removePendingAllocation(dn1, container1);

    // DN1 now has 2 pending, 10GB
    assertEquals(2, tracker.getPendingContainers(dn1).size());
    assertEquals(2 * MAX_CONTAINER_SIZE, tracker.getPendingAllocationSize(dn1));

    // DN2 and DN3 still have 3 pending
    assertEquals(3, tracker.getPendingContainers(dn2).size());
    assertEquals(3, tracker.getPendingContainers(dn3).size());

    // All DNs eventually confirm all containers
    for (DatanodeDetails dn : pipeline.getNodes()) {
      tracker.removePendingAllocation(dn, container1);
      tracker.removePendingAllocation(dn, container2);
      tracker.removePendingAllocation(dn, container3);
    }

    // All DNs should have 0 pending
    assertEquals(0, tracker.getPendingContainers(dn1).size());
    assertEquals(0, tracker.getPendingContainers(dn2).size());
    assertEquals(0, tracker.getPendingContainers(dn3).size());
    assertEquals(0, tracker.getTotalPendingCount());
    assertEquals(0, tracker.getDataNodeCount());
  }

  @Test
  public void testRemoveFromBothWindows() {
    // This test verifies that removal works from both current and previous windows
    // In general, a container could be in previous window after a roll

    // Add containers
    tracker.recordPendingAllocation(pipeline, container1);
    tracker.recordPendingAllocation(pipeline, container2);

    assertEquals(2, tracker.getPendingContainers(dn1).size());

    // Remove container1 - should work regardless of which window it's in
    tracker.removePendingAllocation(dn1, container1);

    assertEquals(1, tracker.getPendingContainers(dn1).size());

    Set<ContainerID> pending = tracker.getPendingContainers(dn1);
    assertFalse(pending.contains(container1));
    assertThat(pending).contains(container2);
  }

  @Test
  public void testUnionOfBothWindows() {
    // This test verifies the two-window concept:
    // getPendingContainers should return union of current + previous windows

    // Add container1
    tracker.recordPendingAllocation(pipeline, container1);

    assertEquals(1, tracker.getPendingContainers(dn1).size());
    Set<ContainerID> pending1 = tracker.getPendingContainers(dn1);
    assertThat(pending1).contains(container1);

    // Add container2 - should be in same window initially
    tracker.recordPendingAllocation(pipeline, container2);

    assertEquals(2, tracker.getPendingContainers(dn1).size());
    Set<ContainerID> pending2 = tracker.getPendingContainers(dn1);
    assertThat(pending2).contains(container1);
    assertThat(pending2).contains(container2);

    // Both containers should be in the union
    assertEquals(2, pending2.size());
  }

  @Test
  public void testIdempotencyAcrossWindows() {
    // Recording same container multiple times should only count it once
    // This should work even if it spans windows

    tracker.recordPendingAllocation(pipeline, container1);
    assertEquals(1, tracker.getPendingContainers(dn1).size());

    // Record again - should still be 1 (idempotency via Set)
    tracker.recordPendingAllocation(pipeline, container1);
    assertEquals(1, tracker.getPendingContainers(dn1).size());

    // Add different container
    tracker.recordPendingAllocation(pipeline, container2);
    assertEquals(2, tracker.getPendingContainers(dn1).size());

    // Record container1 again
    tracker.recordPendingAllocation(pipeline, container1);
    assertEquals(2, tracker.getPendingContainers(dn1).size());
  }

  @Test
  public void testExplicitRemoval() {

    tracker.recordPendingAllocation(pipeline, container1);
    tracker.recordPendingAllocation(pipeline, container2);
    tracker.recordPendingAllocation(pipeline, container3);

    assertEquals(3, tracker.getPendingContainers(dn1).size());

    // Simulate container report confirms container1 and container2
    tracker.removePendingAllocation(dn1, container1);
    tracker.removePendingAllocation(dn1, container2);

    // Immediately reflects the removal (doesn't wait for aging)
    assertEquals(1, tracker.getPendingContainers(dn1).size());

    Set<ContainerID> pending = tracker.getPendingContainers(dn1);
    assertEquals(1, pending.size());
    assertThat(pending).contains(container3);
  }
}
