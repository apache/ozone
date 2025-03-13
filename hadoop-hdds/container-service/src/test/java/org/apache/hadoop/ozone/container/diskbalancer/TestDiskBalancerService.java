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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.util.DiskChecker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
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

  private String schemaVersion;
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
    datanodeUuid = UUID.randomUUID().toString();
    volumeSet = new MutableVolumeSet(datanodeUuid, scmId, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    createDbInstancesForTestIfNeeded(volumeSet, scmId, scmId, conf);
  }

  @AfterEach
  public void cleanup() throws IOException {
    BlockUtils.shutdownCache(conf);
    FileUtils.deleteDirectory(testRoot);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testUpdateService(ContainerTestVersionInfo versionInfo) throws Exception {
    setLayoutAndSchemaForTest(versionInfo);
    // Increase volume's usedBytes
    for (StorageVolume volume : volumeSet.getVolumeMap().values()) {
      volume.incrementUsedSpace(volume.getCurrentUsage().getCapacity() / 2);
    }

    ContainerSet containerSet = new ContainerSet(1000);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
            metrics, c -> {
        });
    DiskBalancerServiceTestImpl svc =
        getDiskBalancerService(containerSet, conf, keyValueHandler, null, 1);

    // Set a low bandwidth to delay job
    svc.setShouldRun(true);
    svc.setThreshold(10.0d);
    svc.setBandwidthInMB(1L);
    svc.setParallelThread(5);
    svc.setVersion(DiskBalancerVersion.DEFAULT_VERSION);

    svc.start();

    assertTrue(svc.getDiskBalancerInfo().isShouldRun());
    assertEquals(10, svc.getDiskBalancerInfo().getThreshold(), 0.0);
    assertEquals(1, svc.getDiskBalancerInfo().getBandwidthInMB());
    assertEquals(5, svc.getDiskBalancerInfo().getParallelThread());

    DiskBalancerInfo newInfo = new DiskBalancerInfo(false, 20.0d, 5L, 10);
    svc.refresh(newInfo);

    assertFalse(svc.getDiskBalancerInfo().isShouldRun());
    assertEquals(20, svc.getDiskBalancerInfo().getThreshold(), 0.0);
    assertEquals(5, svc.getDiskBalancerInfo().getBandwidthInMB());
    assertEquals(10, svc.getDiskBalancerInfo().getParallelThread());

    svc.shutdown();
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testPolicyClassInitialization(ContainerTestVersionInfo versionInfo) throws IOException {
    setLayoutAndSchemaForTest(versionInfo);
    ContainerSet containerSet = new ContainerSet(1000);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
            metrics, c -> {
        });
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
      sb.append(base + "/vol" + i);
      sb.append(",");
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

  static List<Integer> createVolumeSet() {
    List<Integer> params = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      params.add(i);
    }
    return params;
  }

  @ParameterizedTest
  @MethodSource("createVolumeSet")
  public void testCalculateBytesToMove(Integer i) throws IOException {
    try {
      conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
          generateVolumeLocation(testRoot.getAbsolutePath(), i));
      volumeSet = new MutableVolumeSet(datanodeUuid, scmId, conf, null,
          StorageVolume.VolumeType.DATA_VOLUME, null);
      createDbInstancesForTestIfNeeded(volumeSet, scmId, scmId, conf);

      double num1 = 0.02;
      List<StorageVolume> volumes = new ArrayList<>();

      for (StorageVolume volume : volumeSet.getVolumesList()) {
        volume.incrementUsedSpace((long) (volume.getCurrentUsage().getCapacity() * num1));
        volumes.add(volume);
        num1 = 0.8;
      }

      ContainerSet containerSet = new ContainerSet(1000);
      ContainerMetrics metrics = ContainerMetrics.create(conf);
      KeyValueHandler keyValueHandler =
          new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
              metrics, c -> {
          });
      DiskBalancerServiceTestImpl svc =
          getDiskBalancerService(containerSet, conf, keyValueHandler, null, 1);
      svc.setShouldRun(true);
      svc.setThreshold(10);
      svc.setQueueSize(2);

      long expectedBytesToMove = calculateExpectedBytesToMove(volumes, svc.getDiskBalancerInfo().getThreshold());

      assertEquals(expectedBytesToMove, svc.getBytesToMove(),
          "The calculated bytes to move should match the expected reduction to meet the threshold.");
    } catch (DiskChecker.DiskOutOfSpaceException e) {
      assertEquals(0, i, "No storage locations configured");
    }
  }

  private long calculateExpectedBytesToMove(List<StorageVolume> volumes, double threshold) {
    long bytesPendingToMove = 0;

    long totalUsedSpace = 0;
    long totalCapacity = 0;

    for (StorageVolume volume : volumes) {
      totalUsedSpace += volume.getCurrentUsage().getUsedSpace();
      totalCapacity += volume.getCurrentUsage().getCapacity();
    }

    if (totalCapacity == 0) {
      return 0;
    }

    double datanodeUtilization = (double) totalUsedSpace / totalCapacity;
    double thresholdFraction = threshold / 100.0;
    double upperLimit = datanodeUtilization + thresholdFraction;

    for (StorageVolume volume : volumes) {
      long usedSpace = volume.getCurrentUsage().getUsedSpace();
      long capacity = volume.getCurrentUsage().getCapacity();
      double volumeUtilization = (double) usedSpace / capacity;

      if (volumeUtilization > upperLimit) {
        long excessData = usedSpace - (long) (upperLimit * capacity);
        bytesPendingToMove += excessData;
      }
    }
    return bytesPendingToMove;
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
    this.schemaVersion = versionInfo.getSchemaVersion();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
  }
}
