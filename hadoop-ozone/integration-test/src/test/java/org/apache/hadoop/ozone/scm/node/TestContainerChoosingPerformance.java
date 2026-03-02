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
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.getIdealUsage;
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.getVolumeUsages;
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.newVolumeFixedUsage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
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
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.MockSpaceUsageSource;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsagePersistence;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.VolumeFixedUsage;
import org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultVolumeContainerChoosingPolicy;
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
 * This class tests the container selection logic performance.
 */
public class TestContainerChoosingPerformance {

  private static final int NUM_VOLUMES = 20;
  private static final int NUM_CONTAINERS = 100000;
  private static final int NUM_THREADS = 10;
  private static final int NUM_ITERATIONS = 10000;
  private static final int MAX_IN_PROGRESS = 100;
  private static final double THRESHOLD = 10.0;

  private static final OzoneConfiguration CONF = new OzoneConfiguration();

  @TempDir
  private Path baseDir;

  private List<HddsVolume> volumes;
  private ContainerSet containerSet;
  private OzoneContainer ozoneContainer;
  private DefaultVolumeContainerChoosingPolicy volumeContainerChoosingPolicy;
  private ExecutorService executor;
  private MutableVolumeSet volumeSet;

  // Simulate containers currently being balanced (in progress)
  private Set<ContainerID> inProgressContainerIDs = ConcurrentHashMap.newKeySet();
  private Map<HddsVolume, Long> deltaMap = new ConcurrentHashMap<>();

  @BeforeEach
  public void setup() throws Exception {
    CONF.set("hdds.datanode.volume.min.free.space", "10MB");
    containerSet = newContainerSet();
    createVolumes();
    createContainers();
    ozoneContainer = mock(OzoneContainer.class);
    ContainerController containerController = new ContainerController(containerSet, null);
    when(ozoneContainer.getController()).thenReturn(containerController);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    volumeContainerChoosingPolicy = new DefaultVolumeContainerChoosingPolicy(new ReentrantLock());
    executor = Executors.newFixedThreadPool(NUM_THREADS);

    // Create a spied MutableVolumeSet and inject the test volumes
    String datanodeUuid = UUID.randomUUID().toString();
    volumeSet = spy(new MutableVolumeSet(datanodeUuid, CONF, null,
        StorageVolume.VolumeType.DATA_VOLUME, null));
    when(volumeSet.getVolumesList())
        .thenReturn(new ArrayList<>(volumes));
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
  }

  @Test
  @Timeout(300)
  public void testConcurrentContainerChoosingPerformance() throws Exception {
    testContainerSelectionPerformance();
  }

  @Test
  public void testContainerDeletionAfterIteratorGeneration() throws Exception {
    // Test that chooseContainer skips in-progress containers and handles deleted containers.
    inProgressContainerIDs.clear();

    ChooseContainerContext ctx = getChooseContainerContext();

    // Get source containers in policy order
    List<Container<?>> policyOrder = getSourceContainersInPolicyOrder(ctx.srcVolume);

    // choose first container
    ContainerData first = volumeContainerChoosingPolicy.chooseContainer(ozoneContainer, ctx.srcVolume, ctx.dstVolume,
        ctx.dstUsage, inProgressContainerIDs, ctx.upperThreshold);
    assertNotNull(first, "Expected to choose a container on first call");
    assertEquals(policyOrder.get(0).getContainerData().getContainerID(), first.getContainerID());

    // Mark first as in-progress and remove second from containerSet
    inProgressContainerIDs.add(ContainerID.valueOf(first.getContainerID()));
    ozoneContainer.getContainerSet().removeContainer(policyOrder.get(1).getContainerData().getContainerID());

    // Second call: policy skips get(0) (in-progress) and get(1) (deleted), returns get(2)
    ContainerData secondContainer = volumeContainerChoosingPolicy.
        chooseContainer(ozoneContainer, ctx.srcVolume, ctx.dstVolume,
        ctx.dstUsage, inProgressContainerIDs, ctx.upperThreshold);
    assertNotNull(secondContainer, "Expected to choose a container on second call");
    assertEquals(policyOrder.get(2).getContainerData().getContainerID(), secondContainer.getContainerID());
  }

  /**
   * Tests container selection performance using chooseContainer directly.
   */
  private void testContainerSelectionPerformance() throws Exception {
    inProgressContainerIDs.clear();
    deltaMap.clear();

    ChooseContainerContext ctx = getChooseContainerContext();

    CountDownLatch latch = new CountDownLatch(NUM_THREADS);
    AtomicInteger containerChosenCount = new AtomicInteger(0);
    AtomicInteger containerNotChosenCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);
    AtomicLong totalTimeNanos = new AtomicLong(0);

    for (int i = 0; i < NUM_THREADS; i++) {
      executor.submit(() -> {
        try {
          long threadStart = System.nanoTime();
          int containerChosen = 0;
          int containerNotChosen = 0;
          int failures = 0;

          for (int j = 0; j < NUM_ITERATIONS; j++) {
            try {
              ContainerData c = volumeContainerChoosingPolicy.chooseContainer(ozoneContainer,
                  ctx.srcVolume, ctx.dstVolume, ctx.dstUsage, inProgressContainerIDs, ctx.upperThreshold);
              if (c == null) {
                containerNotChosen++;
              } else {
                containerChosen++;
                if (inProgressContainerIDs.size() < MAX_IN_PROGRESS) {
                  inProgressContainerIDs.add(ContainerID.valueOf(c.getContainerID()));
                }
              }
            } catch (Exception e) {
              failures++;
            }
          }

          long threadEnd = System.nanoTime();
          totalTimeNanos.addAndGet(threadEnd - threadStart);
          containerChosenCount.addAndGet(containerChosen);
          containerNotChosenCount.addAndGet(containerNotChosen);
          failureCount.addAndGet(failures);
        } finally {
          latch.countDown();
        }
      });
    }

    // Wait max 5 minutes for test completion
    assertTrue(latch.await(5, TimeUnit.MINUTES), "Test timed out");

    long totalOperations = (long) NUM_THREADS * NUM_ITERATIONS;
    double avgTimePerOp = (double) totalTimeNanos.get() / totalOperations;
    double opsPerSec = totalOperations / (totalTimeNanos.get() / 1_000_000_000.0);

    System.out.println("Container selection performance results:");
    System.out.println("Total volumes: " + NUM_VOLUMES);
    System.out.println("Total containers: " + NUM_CONTAINERS);
    System.out.println("Total threads: " + NUM_THREADS);
    System.out.println("Total operations: " + totalOperations);
    System.out.println("Container Chosen operations: " + containerChosenCount.get());
    System.out.println("Container Not Chosen operations: " + containerNotChosenCount.get());
    System.out.println("Failed operations: " + failureCount.get());
    System.out.println("Total time (ms): " + totalTimeNanos.get() / 1_000_000);
    System.out.println("Average time per operation (ns): " + avgTimePerOp);
    System.out.println("Operations per second: " + opsPerSec);
    assertTrue(containerChosenCount.get() > 0, "Expected at least some containers to be chosen");
  }

  private ChooseContainerContext getChooseContainerContext() {
    List<VolumeFixedUsage> volumeUsages = getVolumeUsages(volumeSet, deltaMap);
    volumeUsages.sort(Comparator.comparingDouble(VolumeFixedUsage::getUtilization)
        .thenComparing(v -> v.getVolume().getStorageID()));
    double idealUsage = getIdealUsage(volumeUsages);
    double upperThreshold = idealUsage + THRESHOLD / 100.0;
    HddsVolume srcVolume = volumeUsages.get(volumeUsages.size() - 1).getVolume();
    HddsVolume dstVolume = volumeUsages.get(0).getVolume();
    VolumeFixedUsage dstUsage = newVolumeFixedUsage(dstVolume, deltaMap);
    return new ChooseContainerContext(srcVolume, dstVolume, dstUsage, upperThreshold);
  }

  private List<Container<?>> getSourceContainersInPolicyOrder(HddsVolume srcVolume) {
    List<Container<?>> list = new ArrayList<>();
    ozoneContainer.getController().getContainers(srcVolume).forEachRemaining(list::add);
    return list.stream()
        .filter(c -> c.getContainerData().isClosed() && c.getContainerData().getBytesUsed() > 0)
        .collect(Collectors.toList());
  }

  private static final class ChooseContainerContext {
    private final HddsVolume srcVolume;
    private final HddsVolume dstVolume;
    private final VolumeFixedUsage dstUsage;
    private final double upperThreshold;

    ChooseContainerContext(HddsVolume srcVolume, HddsVolume dstVolume,
        VolumeFixedUsage dstUsage, double upperThreshold) {
      this.srcVolume = srcVolume;
      this.dstVolume = dstVolume;
      this.dstUsage = dstUsage;
      this.upperThreshold = upperThreshold;
    }
  }

  public void createVolumes() throws IOException {
    // Create volumes with mocked space usage
    volumes = new ArrayList<>();
    for (int i = 0; i < NUM_VOLUMES; i++) {
      String volumePath = baseDir.resolve("disk" + i).toString();
      SpaceUsageSource source = MockSpaceUsageSource.fixed(1000000000, 1000000000 - i * 50000);
      SpaceUsageCheckFactory factory = MockSpaceUsageCheckFactory.of(
          source, Duration.ZERO, SpaceUsagePersistence.None.INSTANCE);
      HddsVolume volume = new HddsVolume.Builder(volumePath)
          .conf(CONF)
          .usageCheckFactory(factory)
          .build();
      volumes.add(volume);
    }
  }

  public void createContainers() {
    List<ContainerID> closedContainerIDs = new ArrayList<>();
    Random random = new Random();
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < NUM_CONTAINERS; i++) {
      boolean isOpen = i < 10; // First 10 containers are open
      int volumeIndex = i % NUM_VOLUMES; // Distribute containers across volumes
      HddsVolume volume = volumes.get(volumeIndex);

      KeyValueContainerData containerData = new KeyValueContainerData(
          i, ContainerLayoutVersion.FILE_PER_BLOCK, ContainerTestHelper.CONTAINER_MAX_SIZE,
          UUID.randomUUID().toString(), UUID.randomUUID().toString());

      containerData.setState(isOpen ? ContainerDataProto.State.OPEN : ContainerDataProto.State.CLOSED);
      containerData.setVolume(volume);
      // Set some bytes used for containers so they can be chosen for disk balancing
      // Use a small non-zero value to ensure containers are not skipped
      long bytesUsed = isOpen ? 0 : (i % 1000 + 1) * 1024L; // 1KB to 1MB for closed containers
      containerData.getStatistics().setBlockBytesForTesting(bytesUsed);
      KeyValueContainer container = new KeyValueContainer(containerData, CONF);

      try {
        containerSet.addContainer(container); // Add container to ContainerSet
      } catch (Exception e) {
        Assertions.fail(e.getMessage());
      }

      // Collect IDs of closed containers
      if (!isOpen) {
        closedContainerIDs.add(ContainerID.valueOf((long) i));
      }
    }

    // Randomly select NUM_THREADS closed containers to be in-progress
    Collections.shuffle(closedContainerIDs, random);
    inProgressContainerIDs.addAll(closedContainerIDs.subList(0, NUM_THREADS));
    System.out.println("Created " +  NUM_CONTAINERS + " containers in " +
        (System.currentTimeMillis() - startTime) + " ms");
  }
}
