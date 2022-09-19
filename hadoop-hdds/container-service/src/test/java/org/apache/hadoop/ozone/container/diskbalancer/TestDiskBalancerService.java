/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.diskbalancer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.TestBlockDeletingService;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singletonMap;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDbInstancesForTestIfNeeded;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests to test diskBalancer service.
 */
@RunWith(Parameterized.class)
public class TestDiskBalancerService {
  private File testRoot;
  private String scmId;
  private String datanodeUuid;
  private OzoneConfiguration conf;

  private final ContainerLayoutVersion layout;
  private final String schemaVersion;
  private MutableVolumeSet volumeSet;

  public TestDiskBalancerService(ContainerTestVersionInfo versionInfo) {
    this.layout = versionInfo.getLayout();
    this.schemaVersion = versionInfo.getSchemaVersion();
    conf = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ContainerTestVersionInfo.versionParameters();
  }

  @Before
  public void init() throws IOException {
    testRoot = GenericTestUtils
        .getTestDir(TestBlockDeletingService.class.getSimpleName());
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

  @After
  public void cleanup() throws IOException {
    BlockUtils.shutdownCache(conf);
    FileUtils.deleteDirectory(testRoot);
  }

  @Test
  public void testBandwidthInMB() throws Exception {
    // Increase volume's usedBytes
    for (StorageVolume volume : volumeSet.getVolumeMap().values()) {
      volume.incrementUsedSpace(volume.getCapacity() / 2);
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
    svc.setBandwidthInMB(1L);
    svc.setBalancedBytesInLastWindow(10 * 1024 * 1024L);

    svc.start();
    DiskBalancerServiceMetrics diskBalancerServiceMetrics = svc.getMetrics();
    GenericTestUtils.waitFor(svc::isStarted, 100, 3000);

    // Dry run to calculate next available time
    balanceAndWait(svc, 1, 3000);

    long originIdleLoopExceedsBandwidthCount =
        diskBalancerServiceMetrics.getIdleLoopExceedsBandwidthCount();
    balanceAndWait(svc, 2, 3000);

    Assert.assertEquals(
        diskBalancerServiceMetrics.getIdleLoopExceedsBandwidthCount() -
            originIdleLoopExceedsBandwidthCount, 1);

    svc.shutdown();
  }

  @Test
  public void testThreshold() throws Exception {
    // Increase volume's usedBytes
    for (StorageVolume volume : volumeSet.getVolumeMap().values()) {
      volume.incrementUsedSpace(volume.getCapacity() / 2);
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
    svc.start();
    DiskBalancerServiceMetrics diskBalancerServiceMetrics = svc.getMetrics();
    GenericTestUtils.waitFor(svc::isStarted, 100, 3000);

    long originRunningLoopCount =
        diskBalancerServiceMetrics.getRunningLoopCount();
    long originIdleLoopCount =
        diskBalancerServiceMetrics.getIdleLoopNoAvailableVolumePairCount();
    // Default threshold is 10%, since two volumes' usage are equal,
    // there won't be balance tasks
    balanceAndWait(svc, 1, 3000);

    Assert.assertEquals(diskBalancerServiceMetrics.getRunningLoopCount() -
            originRunningLoopCount,
        diskBalancerServiceMetrics.getIdleLoopNoAvailableVolumePairCount() -
            originIdleLoopCount);

    svc.shutdown();
  }

  @Test
  public void testDiskBalance() throws Exception {
    // Increase volume's usedBytes
    for (StorageVolume volume : volumeSet.getVolumeMap().values()) {
      volume.incrementUsedSpace(volume.getCapacity() / 2);
    }

    ContainerSet containerSet = new ContainerSet(1000);
    ContainerMetrics metrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler =
        new KeyValueHandler(conf, datanodeUuid, containerSet, volumeSet,
            metrics, c -> {
        });

    ContainerController containerController = new ContainerController(
        containerSet,
        singletonMap(ContainerProtos.ContainerType.KeyValueContainer,
            keyValueHandler));

    // Create one container
    long containerID = ContainerTestHelper.getTestContainerID();
    KeyValueContainerData data =
        new KeyValueContainerData(containerID, layout,
            ContainerTestHelper.CONTAINER_MAX_SIZE,
            UUID.randomUUID().toString(), datanodeUuid);
    data.closeContainer();
    data.setSchemaVersion(schemaVersion);
    KeyValueContainer container = new KeyValueContainer(data, conf);
    container.create(volumeSet,
        new RoundRobinVolumeChoosingPolicy(), scmId);
    containerSet.addContainer(container);
    data = (KeyValueContainerData) containerSet.getContainer(
        containerID).getContainerData();
    data.setBytesUsed(ContainerTestHelper.CONTAINER_MAX_SIZE);
    data.getVolume().incrementUsedSpace(data.getBytesUsed());

    HddsVolume sourceVolume = null, destVolume = null;
    for (StorageVolume volume: volumeSet.getVolumesList()) {
      if ((HddsVolume) volume == data.getVolume()) {
        sourceVolume = (HddsVolume) volume;
      } else {
        destVolume = (HddsVolume) volume;
      }
    }

    Assert.assertTrue(sourceVolume.getUsedSpace() > destVolume.getUsedSpace());

    DiskBalancerServiceTestImpl svc =
        getDiskBalancerService(containerSet, conf, keyValueHandler,
            containerController, 1);
    svc.setShouldRun(true);
    svc.setThreshold(0);
    svc.start();
    DiskBalancerServiceMetrics diskBalancerServiceMetrics = svc.getMetrics();
    GenericTestUtils.waitFor(svc::isStarted, 100, 3000);

    long originRunningLoopCount =
        diskBalancerServiceMetrics.getRunningLoopCount();
    long originSuccessCount =
        diskBalancerServiceMetrics.getSuccessCount();

    balanceAndWait(svc, 1, 3000);

    GenericTestUtils.waitFor(() ->
        svc.getMetrics().getSuccessCount() - originSuccessCount == 1,
        100, 300000);

    Assert.assertEquals(1, diskBalancerServiceMetrics.getRunningLoopCount() -
        originRunningLoopCount);

    Assert.assertTrue(((KeyValueContainerData) containerSet
        .getContainer(containerID).getContainerData())
        .getVolume() != sourceVolume);
    Assert.assertTrue(((KeyValueContainerData) containerSet
        .getContainer(containerID).getContainerData())
        .getVolume() == destVolume);

    svc.shutdown();
  }

  private String generateVolumeLocation(String base, int volumeCount) {
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

  /**
   *  Run service runDeletingTasks and wait for it's been processed.
   */
  private void balanceAndWait(DiskBalancerServiceTestImpl service,
      int timesOfProcessed, int timeoutMs)
      throws TimeoutException, InterruptedException {
    service.runBalanceTasks();
    GenericTestUtils.waitFor(()
        -> service.getTimesOfProcessed() == timesOfProcessed, 100, timeoutMs);
  }
}
