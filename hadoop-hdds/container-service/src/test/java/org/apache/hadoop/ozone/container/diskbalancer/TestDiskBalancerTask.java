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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests the container move logic within DiskBalancerTask.
 */
@Timeout(60)
public class TestDiskBalancerTask {
  @TempDir
  private Path tmpDir;

  private File testRoot;
  private final String scmId = UUID.randomUUID().toString();
  private final String datanodeUuid = UUID.randomUUID().toString();
  private final OzoneConfiguration conf = new OzoneConfiguration();

  private OzoneContainer ozoneContainer;
  private ContainerSet containerSet;
  private ContainerController controller;
  private MutableVolumeSet volumeSet;
  private HddsVolume sourceVolume;
  private HddsVolume destVolume;
  private DiskBalancerServiceTestImpl diskBalancerService;

  private final FaultInjector faultInjector = new TestFaultInjector();

  private static final long CONTAINER_ID = 1L;
  private static final long CONTAINER_SIZE = 1024L * 1024L; // 1 MB

  /**
   * A simple FaultInjector implementation for testing.
   */
  private static class TestFaultInjector extends FaultInjector {
    private Throwable exception;

    @Override
    public void setException(Throwable e) {
      this.exception = e;
    }

    @Override
    public Throwable getException() {
      return exception;
    }

    @Override
    public void reset() {
      this.exception = null;
    }
  }

  @BeforeEach
  public void setup() throws Exception {
    testRoot = tmpDir.toFile();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testRoot.getAbsolutePath());

    // Setup with 2 volumes
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        testRoot.getAbsolutePath() + "/vol1," + testRoot.getAbsolutePath()
            + "/vol2");
    volumeSet = new MutableVolumeSet(datanodeUuid, scmId, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    createDbInstancesForTestIfNeeded(volumeSet, scmId, scmId, conf);

    containerSet = ContainerSet.newReadOnlyContainerSet(1000);
    ContainerMetrics containerMetrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler = new KeyValueHandler(conf, datanodeUuid,
        containerSet, volumeSet, containerMetrics, c -> {
    }, new ContainerChecksumTreeManager(conf));

    Map<ContainerProtos.ContainerType, Handler> handlers = new HashMap<>();
    handlers.put(ContainerProtos.ContainerType.KeyValueContainer, keyValueHandler);
    controller = new ContainerController(containerSet, handlers);
    ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    when(ozoneContainer.getVolumeSet()).thenReturn(volumeSet);
    when(ozoneContainer.getController()).thenReturn(controller);
    when(ozoneContainer.getDispatcher())
        .thenReturn(mock(ContainerDispatcher.class));

    diskBalancerService = new DiskBalancerServiceTestImpl(ozoneContainer,
        100, conf, 1);

    List<StorageVolume> volumes = volumeSet.getVolumesList();
    sourceVolume = (HddsVolume) volumes.get(0);
    destVolume = (HddsVolume) volumes.get(1);

    KeyValueContainer.setInjector(faultInjector);
  }

  @AfterEach
  public void cleanup() throws IOException {
    if (diskBalancerService != null) {
      diskBalancerService.shutdown();
    }

    BlockUtils.shutdownCache(conf);
    if (volumeSet != null) {
      volumeSet.shutdown();
    }
    if (testRoot.exists()) {
      FileUtils.deleteDirectory(testRoot);
    }

    faultInjector.reset();
    KeyValueContainer.setInjector(null);
    ContainerSet.setFaultInjector(null);
  }

  @Test
  public void moveSuccess() throws IOException {
    Container container = createContainer(CONTAINER_ID, sourceVolume);
    long initialSourceUsed = sourceVolume.getCurrentUsage().getUsedSpace();
    long initialDestUsed = destVolume.getCurrentUsage().getUsedSpace();
    String oldContainerPath = container.getContainerData().getContainerPath();

    DiskBalancerService.DiskBalancerTask task = getTask(container.getContainerData());
    task.call();

    // Asserts
    Container newContainer = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(newContainer, "New Container should exist in containerSet");
    assertNotEquals(container, newContainer, "A new container object should represent the moved container");
    assertEquals(destVolume, newContainer.getContainerData().getVolume(),
        "Container should now belong to destination volume");
    assertEquals(initialSourceUsed - CONTAINER_SIZE,
        sourceVolume.getCurrentUsage().getUsedSpace(), "Source volume usage should decrease");
    assertEquals(initialDestUsed + CONTAINER_SIZE,
        destVolume.getCurrentUsage().getUsedSpace(), "Dest volume usage should increase");
    assertFalse(new File(oldContainerPath).exists(), "Old container path should be deleted");
    assertTrue(
        new File(newContainer.getContainerData().getContainerPath()).exists(),
        "New container path should exist");
    assertEquals(1,
        diskBalancerService.getMetrics().getSuccessCount());
    assertEquals(CONTAINER_SIZE,
        diskBalancerService.getMetrics().getSuccessBytes());
  }

  @Test
  public void moveFailsOnCopy() throws IOException {
    Container container = createContainer(CONTAINER_ID, sourceVolume);
    long initialSourceUsed = sourceVolume.getCurrentUsage().getUsedSpace();
    long initialDestUsed = destVolume.getCurrentUsage().getUsedSpace();
    String oldContainerPath = container.getContainerData().getContainerPath();

    faultInjector.setException(new IOException("Fault injection: copy failed"));

    DiskBalancerService.DiskBalancerTask task = getTask(container.getContainerData());
    task.call();

    Container originalContainer = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(originalContainer,
        "Container should still be in containerSet");
    assertEquals(container, originalContainer, "original container should remain unchanged");
    assertEquals(sourceVolume,
        originalContainer.getContainerData().getVolume(),
        "Container should still belong to source volume");
    assertEquals(initialSourceUsed, sourceVolume.getCurrentUsage().getUsedSpace(),
        "Source volume usage should be unchanged");
    assertEquals(initialDestUsed, destVolume.getCurrentUsage().getUsedSpace(),
        "Destination volume usage should be unchanged");
    assertTrue(new File(oldContainerPath).exists(),
        "Original container path should still exist");
    Path tempDir = destVolume.getTmpDir().toPath()
        .resolve(DiskBalancerService.DISK_BALANCER_DIR);
    assertFalse(Files.exists(tempDir),
        "Temp directory should be cleaned up");
    assertEquals(1, diskBalancerService.getMetrics().getFailureCount());
  }

  @Test
  public void moveFailsOnAtomicMove() throws IOException {
    Container container = createContainer(CONTAINER_ID, sourceVolume);
    long initialSourceUsed = sourceVolume.getCurrentUsage().getUsedSpace();
    long initialDestUsed = destVolume.getCurrentUsage().getUsedSpace();
    String oldContainerPath = container.getContainerData().getContainerPath();

    // Inject a failure that will be thrown just after the atomic move during container import.
    faultInjector.setException(new IOException("Fault injection: container import failed after atomic move"));

    DiskBalancerService.DiskBalancerTask task = getTask(
        container.getContainerData());
    task.call();

    Container originalContainer = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(originalContainer);
    assertEquals(container, originalContainer, "original container should remain unchanged");
    assertEquals(sourceVolume,
        originalContainer.getContainerData().getVolume(),
        "Container should still belong to original source volume");
    assertEquals(initialSourceUsed, sourceVolume.getCurrentUsage().getUsedSpace());
    assertEquals(initialDestUsed, destVolume.getCurrentUsage().getUsedSpace());
    assertTrue(new File(oldContainerPath).exists());
    Path tempDir = destVolume.getTmpDir().toPath()
        .resolve(DiskBalancerService.DISK_BALANCER_DIR)
        .resolve(String.valueOf(CONTAINER_ID));
    assertFalse(Files.exists(tempDir), "Temp copy should be cleaned up");
    assertEquals(1, diskBalancerService.getMetrics().getFailureCount());
  }

  @Test
  public void moveFailsDuringInMemoryUpdate() throws IOException {
    Container container = createContainer(CONTAINER_ID, sourceVolume);
    long initialSourceUsed = sourceVolume.getCurrentUsage().getUsedSpace();
    long initialDestUsed = destVolume.getCurrentUsage().getUsedSpace();
    String oldContainerPath = container.getContainerData().getContainerPath();

    // Use the fault injector to fail the final
    // in-memory update after container import
    faultInjector.setException(
        new IOException("Fault Injection: updateContainer failed"));

    DiskBalancerService.DiskBalancerTask task = getTask(
        container.getContainerData());
    task.call();

    // Asserts for rollback
    // The move succeeded on disk but should be reverted by the catch block
    Container originalContainer = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(originalContainer,
        "Container should remain in containerSet");
    assertEquals(container, originalContainer, "original container should remain unchanged");
    assertEquals(sourceVolume,
        originalContainer.getContainerData().getVolume(),
        "Container should still belong to original source volume");
    assertEquals(initialSourceUsed, sourceVolume.getCurrentUsage().getUsedSpace(),
        "Source volume usage should be unchanged");
    assertEquals(initialDestUsed, destVolume.getCurrentUsage().getUsedSpace(),
        "Dest volume usage should be reverted");
    assertTrue(new File(oldContainerPath).exists(),
        "Original container files should not be deleted");

    // Verify the partially moved container at destination is cleaned up
    String idDir = container.getContainerData().getOriginNodeId();
    Path finalDestPath = Paths.get(
        KeyValueContainerLocationUtil.getBaseContainerLocation(
            destVolume.getHddsRootDir().toString(), idDir,
            container.getContainerData().getContainerID()));
    assertFalse(Files.exists(finalDestPath),
        "Moved container at destination should be cleaned up on failure");
    assertEquals(1, diskBalancerService.getMetrics().getFailureCount());
  }

  private KeyValueContainer createContainer(long containerId, HddsVolume vol)
      throws IOException {
    KeyValueContainerData containerData = new KeyValueContainerData(
        containerId, ContainerLayoutVersion.FILE_PER_BLOCK, CONTAINER_SIZE,
        UUID.randomUUID().toString(), datanodeUuid);
    containerData.setState(State.CLOSED);
    containerData.getStatistics().setBlockBytesForTesting(CONTAINER_SIZE);

    KeyValueContainer container = new KeyValueContainer(containerData, conf);
    VolumeChoosingPolicy policy = mock(VolumeChoosingPolicy.class);
    when(policy.chooseVolume(any(List.class), any(Long.class)))
        .thenReturn(vol);
    container.create((VolumeSet) volumeSet, policy, scmId);
    containerSet.addContainer(container);

    // Manually update volume usage for test purposes
    vol.incrementUsedSpace(containerData.getBytesUsed());
    return container;
  }

  private DiskBalancerService.DiskBalancerTask getTask(ContainerData data) {
    return diskBalancerService.createDiskBalancerTask(data, sourceVolume,
        destVolume);
  }
}
