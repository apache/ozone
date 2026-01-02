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

package org.apache.hadoop.ozone.container.diskbalancer;

import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDbInstancesForTestIfNeeded;
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.getVolumeUsages;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DiskBalancerRunningStatus;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerVolumeCalculation.VolumeFixedUsage;
import org.apache.hadoop.ozone.container.diskbalancer.policy.ContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.diskbalancer.policy.DiskBalancerVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * This is a test class for DiskBalancerService.
 */
@Timeout(30)
public class TestDiskBalancerService {
  @TempDir
  private Path tmpDir;

  private File testRoot;
  private String scmId;
  private String datanodeUuid;
  private OzoneConfiguration conf = new OzoneConfiguration();

  private MutableVolumeSet volumeSet;

  @BeforeEach
  public void init() throws IOException {
    testRoot = tmpDir.toFile();
    if (testRoot.exists()) {
      FileUtils.cleanDirectory(testRoot);
    }
    scmId = UUID.randomUUID().toString();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        generateVolumeLocation(testRoot.getAbsolutePath(), 2));
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testRoot.getAbsolutePath());
    conf.set("hdds.datanode.du.factory.classname",
        "org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory$HalfTera");
    conf.setTimeDuration("hdds.datanode.disk.balancer.service.interval", 2, TimeUnit.SECONDS);
    datanodeUuid = UUID.randomUUID().toString();
    volumeSet = new MutableVolumeSet(datanodeUuid, scmId, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    createDbInstancesForTestIfNeeded(volumeSet, scmId, scmId, conf);
  }

  @AfterEach
  public void cleanup() throws IOException {
    BlockUtils.shutdownCache(conf);
    FileUtils.deleteDirectory(testRoot);
    volumeSet.shutdown();
  }

  /**
   * Creates stale diskBalancer directories to simulate leftover directories
   * from previous failed container moves.
   *
   * @param volumeSet the volume set containing volumes to create stale dirs for
   * @param clusterId the cluster ID to use when constructing paths for uninitialized volumes
   * @throws IOException if directory creation fails
   */
  private void createStaleDiskBalancerDirs(VolumeSet volSet, String clusterId)
      throws IOException {
    List<StorageVolume> volumes = volSet.getVolumesList();
    for (StorageVolume volume : volumes) {
      if (volume instanceof HddsVolume) {
        HddsVolume hddsVolume = (HddsVolume) volume;
        File staleDiskBalancerDir;
        
        File volumeTmpDir = hddsVolume.getTmpDir();
        if (volumeTmpDir != null) {
          // If tmpDir is initialized, use it directly
          staleDiskBalancerDir = new File(volumeTmpDir, DiskBalancerService.DISK_BALANCER_DIR);
        } else {
          // If tmpDir is not initialized, construct the path manually
          File clusterIdDir = new File(hddsVolume.getHddsRootDir(), clusterId);
          File tmpDirPath = new File(clusterIdDir, "tmp");
          staleDiskBalancerDir = new File(tmpDirPath, DiskBalancerService.DISK_BALANCER_DIR);
        }
        
        // Create stale directory with some content
        assertTrue(staleDiskBalancerDir.mkdirs(),
            "Failed to create stale diskBalancer directory: " + staleDiskBalancerDir.getAbsolutePath());
        File staleContainerDir = new File(staleDiskBalancerDir, "12345");
        assertTrue(staleContainerDir.mkdirs());
        // Verify stale directory exists before cleanup
        assertTrue(staleDiskBalancerDir.exists(),
            "Stale diskBalancer directory should exist before cleanup");
      }
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testUpdateService(ContainerTestVersionInfo versionInfo) throws Exception {
    setLayoutAndSchemaForTest(versionInfo);
    // Increase volume's usedBytes
    for (StorageVolume volume : volumeSet.getVolumeMap().values()) {
      volume.incrementUsedSpace(volume.getCurrentUsage().getCapacity() / 2);
    }

    ContainerSet containerSet = ContainerSet.newReadOnlyContainerSet(1000);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
            metrics, c -> {
        }, new ContainerChecksumTreeManager(conf));
    DiskBalancerServiceTestImpl svc =
        getDiskBalancerService(containerSet, conf, keyValueHandler, null, 1);

    // Set a low bandwidth to delay job
    DiskBalancerInfo initialInfo = new DiskBalancerInfo(DiskBalancerRunningStatus.RUNNING, 10.0d, 1L, 5,
        true, DiskBalancerVersion.DEFAULT_VERSION);
    svc.refresh(initialInfo);

    svc.start();

    assertTrue(svc.getDiskBalancerInfo().isShouldRun());
    assertEquals(10, svc.getDiskBalancerInfo().getThreshold(), 0.0);
    assertEquals(1, svc.getDiskBalancerInfo().getBandwidthInMB());
    assertEquals(5, svc.getDiskBalancerInfo().getParallelThread());
    assertTrue(svc.getDiskBalancerInfo().isStopAfterDiskEven());

    DiskBalancerInfo newInfo = new DiskBalancerInfo(DiskBalancerRunningStatus.STOPPED, 20.0d, 5L, 10, false);
    svc.refresh(newInfo);

    assertFalse(svc.getDiskBalancerInfo().isShouldRun());
    assertEquals(20, svc.getDiskBalancerInfo().getThreshold(), 0.0);
    assertEquals(5, svc.getDiskBalancerInfo().getBandwidthInMB());
    assertEquals(10, svc.getDiskBalancerInfo().getParallelThread());
    assertFalse(svc.getDiskBalancerInfo().isStopAfterDiskEven());

    svc.shutdown();
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testPolicyClassInitialization(ContainerTestVersionInfo versionInfo) throws IOException {
    setLayoutAndSchemaForTest(versionInfo);
    ContainerSet containerSet = ContainerSet.newReadOnlyContainerSet(1000);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
            metrics, c -> {
        }, new ContainerChecksumTreeManager(conf));
    DiskBalancerServiceTestImpl svc =
        getDiskBalancerService(containerSet, conf, keyValueHandler, null, 1);

    assertTrue(svc.getContainerChoosingPolicy()
        instanceof DefaultContainerChoosingPolicy);
    assertTrue(svc.getVolumeChoosingPolicy()
        instanceof DefaultVolumeChoosingPolicy);
  }

  private String generateVolumeLocation(String base, int volumeCount) {
    if (volumeCount == 0) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < volumeCount; i++) {
      sb.append(base).append("/vol").append(i);
      sb.append(',');
    }
    return sb.substring(0, sb.length() - 1);
  }

  private DiskBalancerServiceTestImpl getDiskBalancerService(
      ContainerSet containerSet, ConfigurationSource config,
      KeyValueHandler keyValueHandler, ContainerController controller,
      int threadCount) throws IOException {
    OzoneContainer ozoneContainer =
        mockDependencies(containerSet, keyValueHandler, controller);
    return new DiskBalancerServiceTestImpl(ozoneContainer, 1000, config,
        threadCount);
  }

  public static Stream<Arguments> values() {
    return Stream.of(
        Arguments.arguments(0, 0, 0),
        Arguments.arguments(1, 0, 0),
        Arguments.arguments(1, 50, 0),
        Arguments.arguments(2, 0, 0),
        Arguments.arguments(2, 10, 0),
        Arguments.arguments(2, 50, 40), // one disk is 50% above average, the other disk is 50% below average
        Arguments.arguments(3, 0, 0),
        Arguments.arguments(3, 10, 0),
        Arguments.arguments(4, 0, 0),
        Arguments.arguments(4, 50, 40) // two disks are 50% above average, the other two disks are 50% below average
    );
  }

  @ParameterizedTest
  @MethodSource("values")
  public void testCalculateBytesToMove(int volumeCount, int deltaUsagePercent,
      long expectedBytesToMovePercent) throws IOException {
    int updatedVolumeCount = volumeCount == 0 ? 1 : volumeCount;
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        generateVolumeLocation(testRoot.getAbsolutePath(), updatedVolumeCount));
    volumeSet = new MutableVolumeSet(datanodeUuid, scmId, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    createDbInstancesForTestIfNeeded(volumeSet, scmId, scmId, conf);
    if (volumeCount == 0) {
      volumeSet.failVolume(((HddsVolume) volumeSet.getVolumesList().get(0)).getHddsRootDir().getAbsolutePath());
    }

    double avgUtilization = 0.5;
    int totalOverUtilisedVolumes = 0;

    List<StorageVolume> volumes = volumeSet.getVolumesList();
    for (int i = 0; i < volumes.size(); i++) {
      StorageVolume vol = volumes.get(i);
      long totalCapacityPerVolume = vol.getCurrentUsage().getCapacity();
      if (i % 2 == 0) {
        vol.incrementUsedSpace((long) (totalCapacityPerVolume * (avgUtilization + deltaUsagePercent / 100.0)));
        totalOverUtilisedVolumes++;
      } else {
        vol.incrementUsedSpace((long) (totalCapacityPerVolume * (avgUtilization - deltaUsagePercent / 100.0)));
      }
    }

    ContainerSet containerSet = ContainerSet.newReadOnlyContainerSet(1000);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
            metrics, c -> {
        }, new ContainerChecksumTreeManager(conf));
    DiskBalancerServiceTestImpl svc =
        getDiskBalancerService(containerSet, conf, keyValueHandler, null, 1);

    long totalCapacity = volumes.isEmpty() ? 0 : volumes.get(0).getCurrentUsage().getCapacity();
    long expectedBytesToMove = (long) Math.ceil(
        (totalCapacity * expectedBytesToMovePercent) / 100.0 * totalOverUtilisedVolumes);

    final List<VolumeFixedUsage> volumeUsages = getVolumeUsages(volumeSet, null);
    // data precision loss due to double data involved in calculation
    assertTrue(Math.abs(expectedBytesToMove - svc.calculateBytesToMove(volumeUsages)) <= 1);
  }

  @Test
  public void testConcurrentTasksNotExceedThreadLimit() throws Exception {
    LogCapturer serviceLog = LogCapturer.captureLogs(DiskBalancerService.class);
    int parallelThread = 3;

    ContainerSet containerSet = ContainerSet.newReadOnlyContainerSet(1000);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
            metrics, c -> {
        }, new ContainerChecksumTreeManager(conf));
    DiskBalancerServiceTestImpl svc =
        getDiskBalancerService(containerSet, conf, keyValueHandler, null, parallelThread);

    // Set operational state to RUNNING
    DiskBalancerInfo info = new DiskBalancerInfo(
        DiskBalancerRunningStatus.RUNNING, 10.0d, 100L, parallelThread,
        false, DiskBalancerVersion.DEFAULT_VERSION);
    svc.refresh(info);

    DiskBalancerVolumeChoosingPolicy volumePolicy = mock(DiskBalancerVolumeChoosingPolicy.class);
    ContainerChoosingPolicy containerPolicy = mock(ContainerChoosingPolicy.class);
    svc.setVolumeChoosingPolicy(volumePolicy);
    svc.setContainerChoosingPolicy(containerPolicy);

    List<StorageVolume> volumes = volumeSet.getVolumesList();
    HddsVolume source = (HddsVolume) volumes.get(0);
    HddsVolume dest = (HddsVolume) volumes.get(1);
    ContainerData containerData = mock(ContainerData.class);

    // Mock unique container IDs to correctly populate the Set
    when(containerData.getContainerID()).thenAnswer(invocation -> System.nanoTime());
    when(containerData.getBytesUsed()).thenReturn(100L);

    when(volumePolicy.chooseVolume(any(), anyDouble(), any(), anyLong())).thenReturn(Pair.of(source, dest));
    when(containerPolicy.chooseContainer(any(), any(), any(), any(), anyDouble(), any(), any()))
        .thenReturn(containerData);

    // Test when no tasks are in progress, it should schedule up to the limit
    BackgroundTaskQueue queue = svc.getTasks();
    assertEquals(parallelThread, queue.size());
    assertEquals(parallelThread, svc.getInProgressContainers().size());

    // Test when in-progress tasks are at the limit, no new tasks are scheduled
    svc.getTasks();
    GenericTestUtils.waitFor(() -> serviceLog.getOutput().contains("No available thread " +
            "for disk balancer service. Current thread count is 3."),
        100, 5000);
  }

  private OzoneContainer mockDependencies(ContainerSet containerSet,
      KeyValueHandler keyValueHandler, ContainerController controller) {
    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    when(ozoneContainer.getWriteChannel()).thenReturn(null);
    ContainerDispatcher dispatcher = mock(ContainerDispatcher.class);
    when(ozoneContainer.getDispatcher()).thenReturn(dispatcher);
    when(dispatcher.getHandler(any())).thenReturn(keyValueHandler);
    when(ozoneContainer.getVolumeSet()).thenReturn(volumeSet);
    when(ozoneContainer.getController()).thenReturn(controller);
    return ozoneContainer;
  }

  private void setLayoutAndSchemaForTest(ContainerTestVersionInfo versionInfo) {
    String schemaVersion = versionInfo.getSchemaVersion();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
  }

  public static Stream<Arguments> thresholdValidationTestCases() {
    return Stream.of(
        // Invalid values that should throw IllegalArgumentException
        Arguments.arguments(0.0, true, null),
        Arguments.arguments(100.0, true, null),
        Arguments.arguments(-1.0, true, null),
        Arguments.arguments(-0.001, true, null),
        Arguments.arguments(100.001, true, null),
        // Valid boundary values that should be accepted
        Arguments.arguments(0.001, false, 0.001),
        Arguments.arguments(99.999, false, 99.999),
        // Valid middle values that should be accepted
        Arguments.arguments(50.5, false, 50.5),
        Arguments.arguments(99.0, false, 99.0)
    );
  }

  @ParameterizedTest
  @MethodSource("thresholdValidationTestCases")
  public void testDiskBalancerConfigurationThresholdValidation(double threshold,
      boolean shouldThrowException, Double expectedThreshold) {
    DiskBalancerConfiguration config = new DiskBalancerConfiguration();

    if (shouldThrowException) {
      IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
          () -> config.setThreshold(threshold));
      assertEquals("Threshold must be a percentage(double) in the range 0 to 100 both exclusive.",
          exception.getMessage());
    } else {
      // Valid threshold should be accepted
      config.setThreshold(threshold);
      assertEquals(expectedThreshold, config.getThreshold(), 0.0001);
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testDiskBalancerCleansUpStaleTmpDir(ContainerTestVersionInfo versionInfo) throws Exception {
    setLayoutAndSchemaForTest(versionInfo);
    // Start volumes to initialize tmp directories
    volumeSet.startAllVolume();

    ContainerSet containerSet = ContainerSet.newReadOnlyContainerSet(1000);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
            metrics, c -> {
        }, new ContainerChecksumTreeManager(conf));

    // Create stale diskBalancer directories to simulate leftover from previous run
    createStaleDiskBalancerDirs(volumeSet, scmId);

    // Use actual DiskBalancerService (not TestImpl) to test the real start() method
    OzoneContainer ozoneContainer = mockDependencies(containerSet, keyValueHandler, null);
    DiskBalancerService svc = new DiskBalancerService(ozoneContainer, 1000, 1000,
        TimeUnit.MILLISECONDS, 1, conf);

    // Start the service, which should clean up stale tmp directories via cleanupTmpDir()
    svc.start();

    // Verify stale diskBalancer tmp directories are cleaned up
    for (StorageVolume volume : volumeSet.getVolumesList()) {
      if (volume instanceof HddsVolume) {
        HddsVolume hddsVolume = (HddsVolume) volume;
        File volumeTmpDir = hddsVolume.getTmpDir();
        File diskBalancerTmpDir = new File(volumeTmpDir, DiskBalancerService.DISK_BALANCER_DIR);

        // Verify stale directory is cleaned up (should not exist)
        assertFalse(diskBalancerTmpDir.exists(),
            "Stale diskBalancer tmp directory should be cleaned up on startup");
      }
    }

    svc.shutdown();
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testDiskBalancerCleanupWhenTmpDirNotInitialized(ContainerTestVersionInfo versionInfo) throws Exception {
    setLayoutAndSchemaForTest(versionInfo);
    // Create a fresh volume set WITHOUT calling createDbInstancesForTestIfNeeded
    // This simulates volumes that are formatted but tmpDir is not initialized
    MutableVolumeSet testVolumeSet = new MutableVolumeSet(datanodeUuid, scmId, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);

    // Format volumes and ensure clusterID directory exists, but DON'T create tmp dirs
    // This simulates the scenario where tmpDir is null
    for (StorageVolume volume : testVolumeSet.getVolumesList()) {
      if (volume instanceof HddsVolume) {
        HddsVolume hddsVolume = (HddsVolume) volume;
        // Format volume to set clusterID
        hddsVolume.format(scmId);
        // Manually create the clusterID directory (needed for tmpDir creation)
        // but don't call createWorkingDir() or createTmpDirs()
        File clusterIdDir = new File(hddsVolume.getHddsRootDir(), scmId);
        if (!clusterIdDir.exists()) {
          assertTrue(clusterIdDir.mkdirs(),
              "Failed to create clusterID directory: " + clusterIdDir.getAbsolutePath());
        }
        // Verify tmpDir is null (not initialized)
        assertNull(hddsVolume.getTmpDir());
      }
    }

    // Create stale diskBalancer directories manually to simulate leftover from failed move
    // This tests the scenario where stale dirs exist even though tmpDir is not initialized
    createStaleDiskBalancerDirs(testVolumeSet, scmId);

    ContainerSet containerSet = ContainerSet.newReadOnlyContainerSet(1000);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, testVolumeSet,
            metrics, c -> {
        }, new ContainerChecksumTreeManager(conf));

    // Use actual DiskBalancerService (not TestImpl) to test the real start() method
    OzoneContainer ozoneContainer = mockDependencies(containerSet, keyValueHandler, null);
    // Override getVolumeSet to return our test volume set
    when(ozoneContainer.getVolumeSet()).thenReturn(testVolumeSet);

    DiskBalancerService svc = new DiskBalancerService(ozoneContainer, 1000, 1000,
        TimeUnit.MILLISECONDS, 1, conf);

    // Start the service - cleanup should handle volumes with uninitialized tmpDir
    // and clean up stale directories even when tmpDir is null
    svc.start();

    // Verify stale directories are cleaned up even though tmpDir is not initialized
    List<StorageVolume> volumes = testVolumeSet.getVolumesList();
    for (StorageVolume volume : volumes) {
      if (volume instanceof HddsVolume) {
        HddsVolume hddsVolume = (HddsVolume) volume;
        // tmpDir should still be null - cleanup doesn't initialize it
        assertNull(hddsVolume.getTmpDir(),
            "tmpDir should not be initialized by cleanup, it will be created lazily");

        // Verify stale diskBalancer directory is cleaned up
        File hddsRootDir = hddsVolume.getHddsRootDir();
        File expectedDiskBalancerTmpDir = new File(new File(hddsRootDir, scmId),
            "tmp" + File.separator + DiskBalancerService.DISK_BALANCER_DIR);
        assertFalse(expectedDiskBalancerTmpDir.exists(),
            "Stale diskBalancer directory should be cleaned up even when tmpDir is not initialized");
      }
    }

    svc.shutdown();
    testVolumeSet.shutdown();
  }
}
