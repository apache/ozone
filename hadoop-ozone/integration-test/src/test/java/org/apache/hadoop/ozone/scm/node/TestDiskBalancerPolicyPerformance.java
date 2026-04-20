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

package org.apache.hadoop.ozone.scm.node;

import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.MockSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsagePersistence;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.VolumeFixedUsage;
import org.apache.hadoop.ozone.container.diskbalancer.policy.ContainerCandidate;
import org.apache.hadoop.ozone.container.diskbalancer.policy.ContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * This class tests the DiskBalancer ContainerChoosingPolicy.
 */
public class TestDiskBalancerPolicyPerformance {

  private static final int NUM_VOLUMES = 20;
  private static final int NUM_CONTAINERS = 100000;
  private static final int NUM_THREADS = 10;
  private static final int NUM_ITERATIONS = 10000;
  private static final int MAX_IN_PROGRESS = 2500;
  private static final double THRESHOLD = 10.0;

  private OzoneConfiguration conf;

  @TempDir
  private Path baseDir;

  private List<HddsVolume> volumes;
  private ContainerSet containerSet;
  private OzoneContainer ozoneContainer;
  private ContainerChoosingPolicy policy;
  private ExecutorService executor;
  private MutableVolumeSet volumeSet;

  // Simulate containers currently being balanced (in progress)
  private Set<ContainerID> inProgressContainerIDs = ConcurrentHashMap.newKeySet();
  private Map<HddsVolume, Long> deltaMap = new ConcurrentHashMap<>();

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.set("hdds.datanode.volume.min.free.space", "20GB");
    conf.setFloat("hdds.datanode.volume.min.free.space.percent", 0.02f);

    volumes = new ArrayList<>();
    containerSet = newContainerSet();
    createVolumes();
    createContainers();
    ozoneContainer = mock(OzoneContainer.class);
    ContainerController containerController = new ContainerController(containerSet, null);
    when(ozoneContainer.getController()).thenReturn(containerController);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    policy = new DefaultContainerChoosingPolicy(new ReentrantLock());
    executor = Executors.newFixedThreadPool(NUM_THREADS);
  }

  @AfterEach
  public void cleanUp() {
    volumes.forEach(HddsVolume::shutdown);

    // Shutdown executor service
    if (executor != null && !executor.isShutdown()) {
      executor.shutdownNow();
    }

    // Clear in-progress container IDs
    inProgressContainerIDs.clear();

    // Clear ContainerSet
    containerSet = null;

    if (volumeSet != null) {
      volumeSet.shutdown();
      volumeSet = null;
    }


  }

  // ---------- Volume selection tests (chooseVolumesAndContainer) ----------

  @Test
  @Timeout(30)
  public void testVolumeChoosingFailureDueToDiskFull() {
    OzoneConfiguration testConf = new OzoneConfiguration();
    testConf.set("hdds.datanode.volume.min.free.space", "990GB");
    for (StorageVolume volume : volumeSet.getVolumesList()) {
      volume.setConf(testConf);
    }
    assertNull(policy.chooseVolumesAndContainer(ozoneContainer, volumeSet,
        deltaMap, inProgressContainerIDs, THRESHOLD));
    assertEquals(NUM_VOLUMES, volumeSet.getVolumesList().size());
  }

  @Test
  @Timeout(300)
  public void testConcurrentContainerChoosingPerformance() throws Exception {
    testDiskBalancerPolicyPerformance("VolumeAndContainerSelection", policy);
  }

  private void testDiskBalancerPolicyPerformance(String testName, ContainerChoosingPolicy pol) throws Exception {
    CountDownLatch latch = new CountDownLatch(NUM_THREADS);
    AtomicInteger pairChosenCount = new AtomicInteger(0);
    AtomicInteger pairNotChosenCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);
    AtomicLong totalTimeNanos = new AtomicLong(0);

    for (int i = 0; i < NUM_THREADS; i++) {
      executor.submit(() -> {
        try {
          int containerCandidateChosen = 0;
          int containerCandidateNotChosen = 0;
          int failures = 0;

          for (int j = 0; j < NUM_ITERATIONS; j++) {
            long threadStart = System.nanoTime();
            try {
              ContainerCandidate candidate = pol.chooseVolumesAndContainer(ozoneContainer,
                  volumeSet, deltaMap, inProgressContainerIDs, THRESHOLD);

              if (candidate == null) {
                containerCandidateNotChosen++;
              } else {
                containerCandidateChosen++;
                ContainerID chosenId = ContainerID.valueOf(candidate.getContainerData().getContainerID());
                if (inProgressContainerIDs.size() < MAX_IN_PROGRESS) {
                  inProgressContainerIDs.add(chosenId);
                }
                deltaMap.compute(candidate.getSourceVolume(), (k, v) -> (v == null ? 0L : v)
                    - candidate.getContainerData().getBytesUsed());
              }
            } catch (Exception e) {
              failures++;
            } finally {
              totalTimeNanos.addAndGet(System.nanoTime() - threadStart);
            }
          }

          pairChosenCount.addAndGet(containerCandidateChosen);
          pairNotChosenCount.addAndGet(containerCandidateNotChosen);
          failureCount.addAndGet(failures);
        } finally {
          latch.countDown();
        }
      });
    }

    assertTrue(latch.await(5, TimeUnit.MINUTES), "Test timed out");

    long totalOperations = (long) NUM_THREADS * NUM_ITERATIONS;
    int selections = pairChosenCount.get();
    int noSelection = pairNotChosenCount.get();
    System.out.println("Performance results for " + testName);
    System.out.println("Total volumes: " + NUM_VOLUMES);
    System.out.println("Total containers: " + NUM_CONTAINERS);
    System.out.println("Total threads: " + NUM_THREADS);
    System.out.println("Total operations: " + totalOperations);
    System.out.println("Container Candidate Chosen (src+dest+container): " + selections);
    System.out.println("No Container Candidate Chosen: " + noSelection);
    System.out.println("Failed: " + failureCount.get());
    System.out.println("Total time (ms): " + totalTimeNanos.get() / 1_000_000);
    System.out.println("Avg time per operation (ns): " + (double) totalTimeNanos.get() / totalOperations);
    System.out.println("Operations per second: " + totalOperations / (totalTimeNanos.get() / 1_000_000_000.0));
  }

  /**
   * Tests that chooseVolumesAndContainer skips in-progress containers and handles
   * containers removed from containerSet during iteration.
   */
  @Test
  public void testContainerDeletionAfterIteratorGeneration() throws Exception {
    inProgressContainerIDs.clear();
    deltaMap.clear();

    ContainerCandidate first = policy.chooseVolumesAndContainer(ozoneContainer,
        volumeSet, deltaMap, inProgressContainerIDs, THRESHOLD);
    assertNotNull(first, "Expected to choose a container on first call");
    long firstContainerId = first.getContainerData().getContainerID();

    inProgressContainerIDs.add(ContainerID.valueOf(firstContainerId));

    List<Long> sourceContainerIds = getSourceContainerIdsInPolicyOrder();
    long secondId = sourceContainerIds.stream()
        .filter(id -> id != firstContainerId)
        .findFirst()
        .orElseThrow(() -> new AssertionError("Expected at least 2 containers on source"));
    containerSet.removeContainerOnlyFromMemory(secondId);

    ContainerCandidate second = policy.chooseVolumesAndContainer(ozoneContainer,
        volumeSet, deltaMap, inProgressContainerIDs, THRESHOLD);
    assertNotNull(second, "Expected to choose a container on second call");
    assertTrue(second.getContainerData().getContainerID() != firstContainerId
        && second.getContainerData().getContainerID() != secondId,
        "Should skip in-progress and deleted, return third container");
  }

  private List<Long> getSourceContainerIdsInPolicyOrder() {
    List<Long> ids = new ArrayList<>();
    List<VolumeFixedUsage> usages = DiskBalancerVolumeCalculation.getVolumeUsages(volumeSet, deltaMap);
    usages.stream()
        .max(Comparator.comparingDouble(VolumeFixedUsage::getUtilization)
            .thenComparing(v -> v.getVolume().getStorageID()))
        .map(VolumeFixedUsage::getVolume)
        .ifPresent(src -> ozoneContainer.getController().getContainers(src)
            .forEachRemaining(c -> {
              if (c.getContainerData().isClosed() && c.getContainerData().getBytesUsed() > 0) {
                ids.add(c.getContainerData().getContainerID());
              }
            }));
    return ids;
  }

  private void createVolumes() throws IOException {
    String initialDnDir = baseDir.resolve("initialDnDir").toFile().getAbsolutePath();
    Files.createDirectories(baseDir.resolve("initialDnDir"));
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, initialDnDir);

    StateContext mockContext = mock(StateContext.class);
    volumeSet = new MutableVolumeSet(UUID.randomUUID().toString(), conf,
        mockContext, StorageVolume.VolumeType.DATA_VOLUME, null);

    Map<String, StorageVolume> newVolumeMap = new ConcurrentHashMap<>();
    Random random = new Random();
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < NUM_VOLUMES; i++) {
      String volumePath = baseDir.resolve("disk" + i).toString();
      long capacity = 1L * 1024 * 1024 * 1024 * 1024; // 1TB

      // Vary utilization from 5% to 55% so we have imbalance to balance
      double utilFraction = 0.05 + (i / (double) NUM_VOLUMES) * 0.5 + random.nextDouble() * 0.05;
      long usedBytes = (long) (capacity * Math.min(utilFraction, 0.55));
      long available = capacity - usedBytes;

      SpaceUsageSource source = MockSpaceUsageSource.fixed(capacity, available);
      SpaceUsageCheckFactory factory = MockSpaceUsageCheckFactory.of(
          source, Duration.ZERO, SpaceUsagePersistence.None.INSTANCE);
      HddsVolume volume = new HddsVolume.Builder(volumePath)
          .conf(conf)
          .usageCheckFactory(factory)
          .build();
      volume.setConf(conf);
      volumes.add(volume);
      newVolumeMap.put(volume.getStorageDir().getPath(), volume);
    }

    volumeSet.setVolumeMapForTesting(newVolumeMap);
    System.out.println("Created " + NUM_VOLUMES + " volumes in " +
        (System.currentTimeMillis() - startTime) + " ms");
  }

  private void createContainers() {
    List<ContainerID> closedContainerIDs = new ArrayList<>();
    Random random = new Random();
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < NUM_CONTAINERS; i++) {
      // ~50 OPEN containers (policy skips) - keep few so most are eligible
      boolean isOpen = i < 50;
      // High-util volumes (index 15-19) get more containers - more source candidates
      int volumeIndex = (i % 2 == 0) ? (i % NUM_VOLUMES) : (15 + (i % 5));
      volumeIndex = Math.min(volumeIndex, NUM_VOLUMES - 1);
      HddsVolume volume = volumes.get(volumeIndex);

      KeyValueContainerData containerData = new KeyValueContainerData(
          i, ContainerLayoutVersion.FILE_PER_BLOCK, ContainerTestHelper.CONTAINER_MAX_SIZE,
          UUID.randomUUID().toString(), UUID.randomUUID().toString());

      containerData.setState(isOpen ? ContainerDataProto.State.OPEN : ContainerDataProto.State.CLOSED);
      containerData.setVolume(volume);

      // Varied sizes: small (1-10MB), medium (20-80MB), large (100-400MB)
      long bytesUsed;
      if (isOpen) {
        bytesUsed = 0;
      } else {
        int sizeIdx = i % 100;
        if (sizeIdx < 2) {
          bytesUsed = 0;  // Policy skips size 0 (~2% of closed)
        } else if (sizeIdx < 60) {
          bytesUsed = (1 + (i % 10)) * 1024 * 1024L;  // 1-10 MB
        } else if (sizeIdx < 90) {
          bytesUsed = (20 + (i % 60)) * 1024 * 1024L;  // 20-80 MB
        } else {
          bytesUsed = (100 + (i % 300)) * 1024 * 1024L;  // 100-400 MB
        }
      }
      containerData.getStatistics().setBlockBytesForTesting(bytesUsed);
      KeyValueContainer container = new KeyValueContainer(containerData, conf);

      try {
        containerSet.addContainer(container); // Add container to ContainerSet
      } catch (Exception e) {
        Assertions.fail(e.getMessage());
      }

      if (!isOpen) {
        closedContainerIDs.add(ContainerID.valueOf((long) i));
      }
    }

    Collections.shuffle(closedContainerIDs, random);
    inProgressContainerIDs.addAll(closedContainerIDs.subList(0, Math.min(NUM_THREADS, closedContainerIDs.size())));
    System.out.println("Created " + NUM_CONTAINERS + " containers in " +
        (System.currentTimeMillis() - startTime) + " ms");
  }
}
