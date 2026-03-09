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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
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
import org.apache.hadoop.ozone.container.diskbalancer.policy.ContainerCandidate;
import org.apache.hadoop.ozone.container.diskbalancer.policy.ContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * This class tests the VolumeChoosingPolicy.
 */
public class TestVolumeChoosingPerformance {

  private static final int NUM_VOLUMES = 20;
  private static final int NUM_THREADS = 10;
  private static final int NUM_ITERATIONS = 10000;
  private static final double THRESHOLD = 10; // 10% threshold

  private OzoneConfiguration conf;

  @TempDir
  private Path baseDir;

  private MutableVolumeSet volumeSet;
  private List<HddsVolume> hddsVolumes;
  private ContainerChoosingPolicy volumeContainerChoosingPolicy;
  private OzoneContainer ozoneContainer;
  private ExecutorService executor;

  // delta sizes for source volumes
  private Map<HddsVolume, Long> deltaSizes = new ConcurrentHashMap<>();
  private Set<ContainerID> inProgressContainerIDs = ConcurrentHashMap.newKeySet();

  @BeforeEach
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    hddsVolumes = new ArrayList<>();
    createVolumes();
    createContainers();
    volumeContainerChoosingPolicy = new DefaultContainerChoosingPolicy(new ReentrantLock());
    executor = Executors.newFixedThreadPool(NUM_THREADS);
  }

  @AfterEach
  public void cleanUp() {
    hddsVolumes.forEach(HddsVolume::shutdown);

    // Shutdown executor service
    if (executor != null && !executor.isShutdown()) {
      executor.shutdownNow();
    }

    // Clear in-progress delta sizes
    if (deltaSizes != null) {
      deltaSizes.clear();
    }

    // Clear VolumeSet
    if (volumeSet != null) {
      volumeSet.shutdown();
      volumeSet = null;
    }
  }

  @Test
  @Timeout(30)
  public void testVolumeChoosingFailureDueToDiskFull() {
    // Create a separate configuration for this test to avoid affecting other tests
    OzoneConfiguration testConf = new OzoneConfiguration();
    // update volume configure, set a huge min free space
    testConf.set("hdds.datanode.volume.min.free.space", "990GB");
    for (StorageVolume volume: volumeSet.getVolumesList()) {
      volume.setConf(testConf);
    }
    assertNull(volumeContainerChoosingPolicy.chooseVolumesAndContainer(ozoneContainer, volumeSet,
        deltaSizes, inProgressContainerIDs, THRESHOLD));
    assertEquals(NUM_VOLUMES, volumeSet.getVolumesList().size());
  }

  @Test
  @Timeout(300)
  public void testConcurrentVolumeChoosingPerformance() throws Exception {
    testPolicyPerformance("VolumeSelectionLogic", volumeContainerChoosingPolicy);
  }

  /**
   * pairChosenCount: Number of successful container/volume choices from the policy.
   * FailureCount: Failures due to any exceptions thrown during choice or null return.
   */
  private void testPolicyPerformance(String policyName, ContainerChoosingPolicy policy) throws Exception {
    CountDownLatch latch = new CountDownLatch(NUM_THREADS);
    AtomicInteger pairChosenCount = new AtomicInteger(0);
    AtomicInteger pairNotChosenCount = new AtomicInteger(0);
    AtomicInteger failureCount = new AtomicInteger(0);
    AtomicLong totalTimeNanos = new AtomicLong(0);

    Random rand = new Random();

    for (int i = 0; i < NUM_THREADS; i++) {
      executor.submit(() -> {
        try {
          int volumeChosen = 0;
          int volumeNotChosen = 0;
          int failures = 0;

          for (int j = 0; j < NUM_ITERATIONS; j++) {
            // Simulate background activity with a 5% probability in each
            // iteration. This mimics other threads changing volume state
            // (e.g., reserving space on a destination) while the policy is
            // being evaluated, testing its behavior in a dynamic environment.
            if (rand.nextDouble() < 0.05 && hddsVolumes.size() >= 2) {
              HddsVolume sourceVolume = hddsVolumes.get(rand.nextInt(hddsVolumes.size()));

              // Randomly pick a destination volume, ensuring it's different from the source
              HddsVolume destVolume;
              do {
                destVolume = hddsVolumes.get(rand.nextInt(hddsVolumes.size()));
              } while (sourceVolume.equals(destVolume));

              long containerSize = rand.nextInt(100 * 1024 * 1024) + 1;

              // Simulate deltaSizes update for the source volume(negative value)
              deltaSizes.compute(sourceVolume, (k, v) -> (v == null ? 0L : v) - containerSize);

              // Simulate committedBytes update for the destination volume (space reserved)
              destVolume.incCommittedBytes(containerSize);
            }

            long threadStart = System.nanoTime();
            try {
              ContainerCandidate candidate = policy.chooseVolumesAndContainer(ozoneContainer,
                  volumeSet, deltaSizes, inProgressContainerIDs, THRESHOLD);

              if (candidate == null) {
                volumeNotChosen++;
              } else {
                volumeChosen++;
                HddsVolume sourceVolume = candidate.getSourceVolume();

                // The policy automatically calls destVolume.incCommittedBytes(containerSize)
                // when a pair is chosen, so we need to simulate the corresponding
                // deltaMap update for the source volume (negative value = space to be freed)
                // This aligns with how DiskBalancerService updates deltaSizes after choosing
                deltaSizes.compute(sourceVolume, (k, v) -> (v == null ? 0L : v)
                    - candidate.getContainerData().getBytesUsed());

                // Note: In a real DiskBalancerService, after a successful container move,
                // the committed bytes on the destination would be adjusted and the delta
                // on the source would be cleared. For this performance test, we simulate
                // the ongoing state changes that occur during disk balancing operations.
              }
            } catch (Exception e) {
              failures++;
            } finally {
              totalTimeNanos.addAndGet(System.nanoTime() - threadStart);
            }
          }

          pairChosenCount.addAndGet(volumeChosen);
          pairNotChosenCount.addAndGet(volumeNotChosen);
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

    System.out.println("Performance results for " + policyName);
    System.out.println("Total volumes: " + NUM_VOLUMES);
    System.out.println("Total threads: " + NUM_THREADS);
    System.out.println("Threshold(%): " + THRESHOLD);
    System.out.println("Total operations: " + totalOperations);
    System.out.println("Volume Pair Chosen operations: " + pairChosenCount.get());
    System.out.println("Volume Pair Not Chosen operations: " + pairNotChosenCount.get());
    System.out.println("Failed operations: " + failureCount.get());
    System.out.println("Total time (ms): " + totalTimeNanos.get() / 1_000_000);
    System.out.println("Average time per operation (ns): " + avgTimePerOp);
    System.out.println("Operations per second: " + opsPerSec);
  }

  private void createVolumes() throws IOException {
    // set a dummy path for initialisation, as we will override its internal volumeMap.
    String initialDnDir = baseDir.resolve("initialDnDir").toFile().getAbsolutePath();
    Files.createDirectories(baseDir.resolve("initialDnDir"));
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, initialDnDir);
    conf.set("hdds.datanode.volume.min.free.space", "20GB");
    conf.setFloat("hdds.datanode.volume.min.free.space.percent", 0.02f);

    StateContext mockContext = mock(StateContext.class);
    volumeSet = new MutableVolumeSet(UUID.randomUUID().toString(), conf,
        mockContext, StorageVolume.VolumeType.DATA_VOLUME, null);

    // This map will replace the one inside 'volumeSet'
    Map<String, StorageVolume> newVolumeMap = new ConcurrentHashMap<>();
    Random random = new Random();
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < NUM_VOLUMES; i++) {
      String volumePath = baseDir.resolve("disk" + i).toString();
      //capacity of each volume is 1TB
      long capacity = 1L * 1024 * 1024 * 1024 * 1024;

      // Distribute available space to create some variation in usage
      // Between 5% and 55%
      long usedBytes = (long) (capacity * (0.05 + random.nextDouble() * 0.5));
      long available = capacity - usedBytes;

      SpaceUsageSource source = MockSpaceUsageSource.fixed(capacity, available);
      SpaceUsageCheckFactory factory = MockSpaceUsageCheckFactory.of(
          source, Duration.ZERO, SpaceUsagePersistence.None.INSTANCE);
      HddsVolume volume = new HddsVolume.Builder(volumePath)
          .conf(conf)
          .usageCheckFactory(factory)
          .build();
      // Ensure configuration is applied to the volume
      volume.setConf(conf);
      hddsVolumes.add(volume);
      newVolumeMap.put(volume.getStorageDir().getPath(), volume);
    }

    // Initialize the volumeSet with the new volume map
    volumeSet.setVolumeMapForTesting(newVolumeMap);
    System.out.println("Created " +  NUM_VOLUMES + " volumes in " +
        (System.currentTimeMillis() - startTime) + " ms");
  }

  private void createContainers() {
    ContainerSet containerSet = newContainerSet();
    Random random = new Random();
    for (int i = 0; i < NUM_VOLUMES * 10; i++) {
      HddsVolume volume = hddsVolumes.get(i % NUM_VOLUMES);
      try {
        long id = 1000L + i;
        long bytesUsed = (random.nextInt(50) + 1) * 1024 * 1024L;
        KeyValueContainerData containerData = new KeyValueContainerData(id,
            ContainerLayoutVersion.FILE_PER_BLOCK, ContainerTestHelper.CONTAINER_MAX_SIZE,
            UUID.randomUUID().toString(), UUID.randomUUID().toString());
        containerData.setState(ContainerDataProto.State.CLOSED);
        containerData.setVolume(volume);
        containerData.getStatistics().setBlockBytesForTesting(bytesUsed);
        KeyValueContainer container = new KeyValueContainer(containerData, conf);
        containerSet.addContainer(container);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    ozoneContainer = mock(OzoneContainer.class);
    ContainerController controller = new ContainerController(containerSet, null);
    when(ozoneContainer.getController()).thenReturn(controller);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
  }
}
