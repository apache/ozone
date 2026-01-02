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

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.CONTAINER_INTERNAL_ERROR;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createDbInstancesForTestIfNeeded;
import static org.apache.hadoop.ozone.container.diskbalancer.DiskBalancerService.DISK_BALANCER_DIR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
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
import org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultContainerChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.assertj.core.api.Fail;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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
  private MutableVolumeSet volumeSet;
  private HddsVolume sourceVolume;
  private HddsVolume destVolume;
  private DiskBalancerServiceTestImpl diskBalancerService;

  private static final long CONTAINER_ID = 1L;
  private static final long CONTAINER_SIZE = 1024L * 1024L; // 1 MB

  private final TestFaultInjector kvFaultInjector = new TestFaultInjector();

  /**
   * A FaultInjector that can be configured to throw an exception on a
   * specific invocation number. This allows tests to target failure points
   * that occur after initial checks.
   */
  private static class TestFaultInjector extends FaultInjector {
    private Throwable exception;
    private int throwOnInvocation = -1; // -1 means never throw
    private int invocationCount = 0;
    private CountDownLatch ready;
    private CountDownLatch wait;

    TestFaultInjector() {
      init();
    }

    @Override
    public void init() {
      this.ready = new CountDownLatch(1);
      this.wait = new CountDownLatch(1);
    }

    @Override
    public void pause() throws IOException {
      ready.countDown();
      try {
        wait.await();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void resume() throws IOException {
      // Make sure injector pauses before resuming.
      try {
        ready.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
        Assertions.assertTrue(Fail.fail("resume interrupted"));
      }
      wait.countDown();
    }

    /**
     * Sets an exception to be thrown on a specific invocation.
     * @param e The exception to throw.
     * @param onInvocation The invocation number to throw on (e.g., 1 for the
     * first call, 2 for the second, etc.).
     */
    public void setException(Throwable e, int onInvocation) {
      this.exception = e;
      this.throwOnInvocation = onInvocation;
      this.invocationCount = 0; // Reset count for each new test setup
    }

    @Override
    public void setException(Throwable e) {
      // Default to throwing on the first invocation if no number is specified.
      setException(e, 1);
    }

    @Override
    public Throwable getException() {
      invocationCount++;
      if (exception != null && invocationCount == throwOnInvocation) {
        return exception;
      }
      return null;
    }

    @Override
    public void reset() {
      this.exception = null;
      this.throwOnInvocation = -1;
      this.invocationCount = 0;
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
    conf.setClass(SpaceUsageCheckFactory.Conf.configKeyForClassName(),
        MockSpaceUsageCheckFactory.HalfTera.class,
        SpaceUsageCheckFactory.class);
    volumeSet = new MutableVolumeSet(datanodeUuid, scmId, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    createDbInstancesForTestIfNeeded(volumeSet, scmId, scmId, conf);

    List<StorageVolume> volumes = volumeSet.getVolumesList();
    sourceVolume = (HddsVolume) volumes.get(0);
    destVolume = (HddsVolume) volumes.get(1);

    // reset volume's usedBytes
    sourceVolume.incrementUsedSpace(0 - sourceVolume.getCurrentUsage().getUsedSpace());
    destVolume.incrementUsedSpace(0 - destVolume.getCurrentUsage().getUsedSpace());
    sourceVolume.incrementUsedSpace(sourceVolume.getCurrentUsage().getCapacity() / 2);

    containerSet = ContainerSet.newReadOnlyContainerSet(1000);
    ContainerMetrics containerMetrics = ContainerMetrics.create(conf);
    KeyValueHandler keyValueHandler = new KeyValueHandler(conf, datanodeUuid,
        containerSet, volumeSet, containerMetrics, c -> {
    }, new ContainerChecksumTreeManager(conf));
    keyValueHandler.setClusterID(scmId);

    Map<ContainerProtos.ContainerType, Handler> handlers = new HashMap<>();
    handlers.put(ContainerProtos.ContainerType.KeyValueContainer, keyValueHandler);
    ContainerController controller = new ContainerController(containerSet, handlers);
    ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    when(ozoneContainer.getVolumeSet()).thenReturn(volumeSet);
    when(ozoneContainer.getController()).thenReturn(controller);
    when(ozoneContainer.getDispatcher())
        .thenReturn(mock(ContainerDispatcher.class));

    DiskBalancerConfiguration diskBalancerConfiguration = conf.getObject(DiskBalancerConfiguration.class);
    diskBalancerConfiguration.setDiskBalancerShouldRun(true);
    conf.setFromObject(diskBalancerConfiguration);
    diskBalancerService = new DiskBalancerServiceTestImpl(ozoneContainer,
        100, conf, 1);
    DiskBalancerService.setReplicaDeletionDelayMills(0);
    KeyValueContainer.setInjector(kvFaultInjector);
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

    kvFaultInjector.reset();
    KeyValueContainer.setInjector(null);
    DiskBalancerService.setInjector(null);
    DefaultContainerChoosingPolicy.setTest(false);
  }

  @ParameterizedTest
  @EnumSource(names = {"CLOSED", "QUASI_CLOSED"})
  public void moveSuccess(State containerState) throws IOException {
    long initialSourceUsed = sourceVolume.getCurrentUsage().getUsedSpace();
    long initialDestUsed = destVolume.getCurrentUsage().getUsedSpace();
    long initialDestCommitted = destVolume.getCommittedBytes();
    long initialSourceDelta = diskBalancerService.getDeltaSizes().get(sourceVolume) == null ?
        0L : diskBalancerService.getDeltaSizes().get(sourceVolume);

    Container container = createContainer(CONTAINER_ID, sourceVolume, containerState);
    State originalState = container.getContainerState();
    assertEquals(initialSourceUsed + CONTAINER_SIZE, sourceVolume.getCurrentUsage().getUsedSpace());
    assertEquals(initialDestUsed, destVolume.getCurrentUsage().getUsedSpace());

    String oldContainerPath = container.getContainerData().getContainerPath();
    if (containerState == State.QUASI_CLOSED) {
      DefaultContainerChoosingPolicy.setTest(true);
    }
    DiskBalancerService.DiskBalancerTask task = getTask();
    task.call();
    assertEquals(State.DELETED, container.getContainerState());

    // Asserts
    Container newContainer = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(newContainer);
    assertNotEquals(container, newContainer);
    assertEquals(originalState, newContainer.getContainerState());
    assertEquals(destVolume, newContainer.getContainerData().getVolume());
    assertEquals(initialSourceUsed, sourceVolume.getCurrentUsage().getUsedSpace());
    assertEquals(initialDestUsed + CONTAINER_SIZE, destVolume.getCurrentUsage().getUsedSpace());
    assertFalse(new File(oldContainerPath).exists());
    assertTrue(new File(newContainer.getContainerData().getContainerPath()).exists());
    assertEquals(1, diskBalancerService.getMetrics().getSuccessCount());
    assertEquals(CONTAINER_SIZE, diskBalancerService.getMetrics().getSuccessBytes());
    assertEquals(initialDestCommitted, destVolume.getCommittedBytes());
    assertEquals(initialSourceDelta, diskBalancerService.getDeltaSizes().get(sourceVolume));
  }

  @ContainerTestVersionInfo.ContainerTest
  public void moveFailsAfterCopy(ContainerTestVersionInfo versionInfo)
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    setLayoutAndSchemaForTest(versionInfo);

    Container container = createContainer(CONTAINER_ID, sourceVolume, State.CLOSED);
    long initialSourceUsed = sourceVolume.getCurrentUsage().getUsedSpace();
    long initialDestUsed = destVolume.getCurrentUsage().getUsedSpace();
    long initialDestCommitted = destVolume.getCommittedBytes();
    long initialSourceDelta = diskBalancerService.getDeltaSizes().get(sourceVolume) == null ?
        0L : diskBalancerService.getDeltaSizes().get(sourceVolume);
    String oldContainerPath = container.getContainerData().getContainerPath();

    // verify temp container directory doesn't exist before task execution
    Path tempContainerDir = destVolume.getTmpDir().toPath()
        .resolve(DISK_BALANCER_DIR).resolve(String.valueOf(CONTAINER_ID));
    File dir = new File(String.valueOf(tempContainerDir));
    assertFalse(dir.exists(), "Temp container directory should not exist before task starts");

    kvFaultInjector.setException(new IOException("Fault injection: copy failed"), 1);
    final TestFaultInjector serviceFaultInjector = new TestFaultInjector();
    DiskBalancerService.setInjector(serviceFaultInjector);
    DiskBalancerService.DiskBalancerTask task = getTask();
    CompletableFuture completableFuture = CompletableFuture.runAsync(() -> task.call());
    GenericTestUtils.waitFor(() -> {
      try {
        return Files.exists(tempContainerDir) && !FileUtils.isEmptyDirectory(tempContainerDir.toFile());
      } catch (IOException e) {
        fail("Failed to check temp container directory existence", e);
      }
      return false;
    }, 100, 30000);
    assertTrue(diskBalancerService.getInProgressContainers().contains(ContainerID.valueOf(CONTAINER_ID)));

    serviceFaultInjector.resume();
    // wait for task to be completed
    completableFuture.get();
    Container originalContainer = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(originalContainer);
    assertEquals(container, originalContainer);
    assertEquals(sourceVolume,
        originalContainer.getContainerData().getVolume());
    assertEquals(initialSourceUsed, sourceVolume.getCurrentUsage().getUsedSpace());
    assertEquals(initialDestUsed, destVolume.getCurrentUsage().getUsedSpace());
    assertTrue(new File(oldContainerPath).exists());
    assertFalse(Files.exists(tempContainerDir), "Temp container directory should be cleaned up");
    assertEquals(1, diskBalancerService.getMetrics().getFailureCount());
    assertEquals(initialDestCommitted, destVolume.getCommittedBytes());
    assertFalse(diskBalancerService.getInProgressContainers().contains(ContainerID.valueOf(CONTAINER_ID)));
    assertEquals(initialSourceDelta, diskBalancerService.getDeltaSizes().get(sourceVolume));
  }

  @ContainerTestVersionInfo.ContainerTest
  public void moveFailsOnAtomicMove(ContainerTestVersionInfo versionInfo)
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    setLayoutAndSchemaForTest(versionInfo);

    Container container = createContainer(CONTAINER_ID, sourceVolume, State.CLOSED);
    long initialSourceUsed = sourceVolume.getCurrentUsage().getUsedSpace();
    long initialDestUsed = destVolume.getCurrentUsage().getUsedSpace();
    long initialDestCommitted = destVolume.getCommittedBytes();
    long initialSourceDelta = diskBalancerService.getDeltaSizes().get(sourceVolume) == null ?
        0L : diskBalancerService.getDeltaSizes().get(sourceVolume);
    String oldContainerPath = container.getContainerData().getContainerPath();
    Path tempDir = destVolume.getTmpDir().toPath()
        .resolve(DISK_BALANCER_DIR)
        .resolve(String.valueOf(CONTAINER_ID));
    assertFalse(Files.exists(tempDir), "Temp container directory should not exist");
    Path destDirPath = Paths.get(
        KeyValueContainerLocationUtil.getBaseContainerLocation(
            destVolume.getHddsRootDir().toString(), scmId,
            container.getContainerData().getContainerID()));
    assertFalse(Files.exists(destDirPath), "Dest container directory should not exist");

    // create dest container directory
    assertTrue(destDirPath.toFile().mkdirs());
    // create one file in the dest container directory
    Path testfile = destDirPath.resolve("testfile");
    assertTrue(testfile.toFile().createNewFile());

    GenericTestUtils.LogCapturer serviceLog = GenericTestUtils.LogCapturer.captureLogs(DiskBalancerService.class);
    final TestFaultInjector serviceFaultInjector = new TestFaultInjector();
    DiskBalancerService.setInjector(serviceFaultInjector);
    DiskBalancerService.DiskBalancerTask task = getTask();
    CompletableFuture completableFuture = CompletableFuture.runAsync(() -> task.call());
    // wait for temp container directory to be created
    GenericTestUtils.waitFor(() -> {
      try {
        return Files.exists(tempDir) && !FileUtils.isEmptyDirectory(tempDir.toFile());
      } catch (IOException e) {
        fail("Failed to check temp container directory existence", e);
      }
      return false;
    }, 100, 30000);
    assertTrue(diskBalancerService.getInProgressContainers().contains(ContainerID.valueOf(CONTAINER_ID)));
    serviceFaultInjector.resume();
    completableFuture.get();

    String expectedString = "Container Directory " + destDirPath + " already exists and are not empty";
    assertTrue(serviceLog.getOutput().contains(expectedString));
    Container originalContainer = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(originalContainer);
    assertEquals(container, originalContainer);
    assertEquals(sourceVolume, originalContainer.getContainerData().getVolume());
    assertEquals(initialSourceUsed, sourceVolume.getCurrentUsage().getUsedSpace());
    assertEquals(initialDestUsed, destVolume.getCurrentUsage().getUsedSpace());
    assertTrue(new File(oldContainerPath).exists());
    assertFalse(Files.exists(tempDir), "Temp copy should be cleaned up");
    assertTrue(Files.exists(destDirPath), "Dest container directory should not be cleaned up");
    assertTrue(testfile.toFile().exists(), "testfile should not be cleaned up");
    assertEquals(1, diskBalancerService.getMetrics().getFailureCount());
    assertEquals(initialDestCommitted, destVolume.getCommittedBytes());
    assertFalse(diskBalancerService.getInProgressContainers().contains(ContainerID.valueOf(CONTAINER_ID)));
    assertEquals(initialSourceDelta, diskBalancerService.getDeltaSizes().get(sourceVolume));
  }

  @ContainerTestVersionInfo.ContainerTest
  public void moveFailsDuringInMemoryUpdate(ContainerTestVersionInfo versionInfo)
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    setLayoutAndSchemaForTest(versionInfo);

    Container container = createContainer(CONTAINER_ID, sourceVolume, State.QUASI_CLOSED);
    long initialSourceUsed = sourceVolume.getCurrentUsage().getUsedSpace();
    long initialDestUsed = destVolume.getCurrentUsage().getUsedSpace();
    long initialDestCommitted = destVolume.getCommittedBytes();
    long initialSourceDelta = diskBalancerService.getDeltaSizes().get(sourceVolume) == null ?
        0L : diskBalancerService.getDeltaSizes().get(sourceVolume);
    String oldContainerPath = container.getContainerData().getContainerPath();
    Path destDirPath = Paths.get(
        KeyValueContainerLocationUtil.getBaseContainerLocation(
            destVolume.getHddsRootDir().toString(), scmId,
            container.getContainerData().getContainerID()));
    assertFalse(Files.exists(destDirPath),
        "Destination container should not exist before task execution");

    ContainerSet spyContainerSet = spy(containerSet);
    doThrow(new StorageContainerException("Mockito spy: updateContainer failed",
        CONTAINER_INTERNAL_ERROR))
        .when(spyContainerSet).updateContainer(any(Container.class));
    when(ozoneContainer.getContainerSet()).thenReturn(spyContainerSet);

    DefaultContainerChoosingPolicy.setTest(true);
    DiskBalancerService.DiskBalancerTask task = getTask();
    CompletableFuture completableFuture = CompletableFuture.runAsync(() -> task.call());

    final TestFaultInjector serviceFaultInjector = new TestFaultInjector();
    DiskBalancerService.setInjector(serviceFaultInjector);
    GenericTestUtils.waitFor(() -> {
      try {
        return Files.exists(destDirPath) && !FileUtils.isEmptyDirectory(destDirPath.toFile());
      } catch (IOException e) {
        fail("Failed to check dest container directory existence", e);
      }
      return false;
    }, 100, 30000);
    assertTrue(diskBalancerService.getInProgressContainers().contains(ContainerID.valueOf(CONTAINER_ID)));
    serviceFaultInjector.resume();
    // wait for task to be completed
    completableFuture.get();

    // Asserts for rollback
    // The move succeeded on disk but should be reverted by the catch block
    Container originalContainer = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(originalContainer);
    assertEquals(container, originalContainer);
    assertEquals(sourceVolume, originalContainer.getContainerData().getVolume());
    assertEquals(initialSourceUsed, sourceVolume.getCurrentUsage().getUsedSpace());
    assertEquals(initialDestUsed, destVolume.getCurrentUsage().getUsedSpace());
    assertTrue(new File(oldContainerPath).exists());
    assertFalse(FileUtils.isEmptyDirectory(new File(oldContainerPath)));
    assertEquals(State.QUASI_CLOSED, originalContainer.getContainerState(),
        "Container state should remain QUASI_CLOSED after rollback");

    // Verify the partially moved container at destination is cleaned up
    assertFalse(Files.exists(destDirPath),
        "Moved container at destination should be cleaned up on failure");
    assertEquals(1, diskBalancerService.getMetrics().getFailureCount());
    assertEquals(initialDestCommitted, destVolume.getCommittedBytes());
    assertFalse(diskBalancerService.getInProgressContainers().contains(ContainerID.valueOf(CONTAINER_ID)));
    assertEquals(initialSourceDelta, diskBalancerService.getDeltaSizes().get(sourceVolume));
  }

  @ContainerTestVersionInfo.ContainerTest
  public void moveFailsDuringOldContainerRemove(ContainerTestVersionInfo versionInfo) throws IOException {
    setLayoutAndSchemaForTest(versionInfo);

    Container container = createContainer(CONTAINER_ID, sourceVolume, State.CLOSED);
    long initialSourceUsed = sourceVolume.getCurrentUsage().getUsedSpace();
    long initialDestUsed = destVolume.getCurrentUsage().getUsedSpace();
    long initialDestCommitted = destVolume.getCommittedBytes();
    long initialSourceDelta = diskBalancerService.getDeltaSizes().get(sourceVolume) == null ?
        0L : diskBalancerService.getDeltaSizes().get(sourceVolume);

    // Use a static mock for the KeyValueContainer utility class
    try (MockedStatic<KeyValueContainerUtil> mockedUtil =
             mockStatic(KeyValueContainerUtil.class, Mockito.CALLS_REAL_METHODS)) {
      // Stub the static method to throw an exception
      mockedUtil.when(() -> KeyValueContainerUtil.removeContainer(
              any(KeyValueContainerData.class), any(OzoneConfiguration.class)))
          .thenThrow(new IOException("Mockito: old container delete() failed"));

      DiskBalancerService.DiskBalancerTask task = getTask();
      task.call();

      // Assertions for successful move despite old container cleanup failure
      assertEquals(1, diskBalancerService.getMetrics().getSuccessCount());
      assertEquals(0, diskBalancerService.getMetrics().getFailureCount());
      assertEquals(CONTAINER_SIZE, diskBalancerService.getMetrics().getSuccessBytes());

      // Verify new container is active on the destination volume
      Container newContainer = containerSet.getContainer(CONTAINER_ID);
      assertNotNull(newContainer);
      assertNotEquals(container, newContainer);
      assertEquals(destVolume, newContainer.getContainerData().getVolume());
      assertTrue(new File(newContainer.getContainerData().getContainerPath()).exists());

      // Verify old container still exists
      assertTrue(new File(container.getContainerData().getContainerPath()).exists());
      assertFalse(FileUtils.isEmptyDirectory(new File(container.getContainerData().getContainerPath())));
      assertEquals(State.DELETED, container.getContainerState());

      // Verify volume usage is updated correctly
      assertEquals(initialSourceUsed,
          sourceVolume.getCurrentUsage().getUsedSpace());
      assertEquals(initialDestUsed + CONTAINER_SIZE,
          destVolume.getCurrentUsage().getUsedSpace());
      assertEquals(initialDestCommitted, destVolume.getCommittedBytes());
      assertFalse(diskBalancerService.getInProgressContainers().contains(ContainerID.valueOf(CONTAINER_ID)));
      assertEquals(initialSourceDelta, diskBalancerService.getDeltaSizes().get(sourceVolume));
    }
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testDestVolumeCommittedSpaceReleased(ContainerTestVersionInfo versionInfo) throws IOException {
    setLayoutAndSchemaForTest(versionInfo);

    createContainer(CONTAINER_ID, sourceVolume, State.CLOSED);
    long initialSourceUsed = sourceVolume.getCurrentUsage().getUsedSpace();
    long initialDestUsed = destVolume.getCurrentUsage().getUsedSpace();
    long initialDestCommitted = destVolume.getCommittedBytes();
    long initialSourceDelta = diskBalancerService.getDeltaSizes().get(sourceVolume) == null ?
        0L : diskBalancerService.getDeltaSizes().get(sourceVolume);

    GenericTestUtils.LogCapturer serviceLog = GenericTestUtils.LogCapturer.captureLogs(DiskBalancerService.class);
    DiskBalancerService.DiskBalancerTask task = getTask();
    long defaultContainerSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);
    // verify committed space is reserved for destination volume
    assertEquals(defaultContainerSize, destVolume.getCommittedBytes() - initialDestCommitted);

    // delete the container from containerSet to simulate a failure
    containerSet.removeContainer(CONTAINER_ID);

    task.call();
    String expectedString = "Container " + CONTAINER_ID + " doesn't exist in ContainerSet";
    assertTrue(serviceLog.getOutput().contains(expectedString));
    Container originalContainer = containerSet.getContainer(CONTAINER_ID);
    assertNull(originalContainer);
    assertEquals(initialSourceUsed, sourceVolume.getCurrentUsage().getUsedSpace());
    assertEquals(initialDestUsed, destVolume.getCurrentUsage().getUsedSpace());
    assertEquals(0, destVolume.getCommittedBytes() - initialDestCommitted);
    assertEquals(1, diskBalancerService.getMetrics().getFailureCount());
    assertEquals(initialDestCommitted, destVolume.getCommittedBytes());
    assertFalse(diskBalancerService.getInProgressContainers().contains(ContainerID.valueOf(CONTAINER_ID)));
    assertEquals(initialSourceDelta, diskBalancerService.getDeltaSizes().get(sourceVolume));
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testOldReplicaDelayedDeletion(ContainerTestVersionInfo versionInfo)
      throws IOException, InterruptedException {
    setLayoutAndSchemaForTest(versionInfo);
    long delay = 2000L; // 2 second delay
    DiskBalancerService.setReplicaDeletionDelayMills(delay);

    Container container = createContainer(CONTAINER_ID, sourceVolume, State.CLOSED);
    KeyValueContainerData keyValueContainerData = (KeyValueContainerData) container.getContainerData();
    File oldContainerDir = new File(keyValueContainerData.getContainerPath());
    assertTrue(oldContainerDir.exists());

    DiskBalancerService.DiskBalancerTask task = getTask();
    task.call();
    assertEquals(State.DELETED, container.getContainerState());
    // Verify that the old container is not deleted immediately
    assertTrue(oldContainerDir.exists());

    // create another container to trigger the deletion of old replicas
    createContainer(CONTAINER_ID + 1, sourceVolume, State.CLOSED);
    task = getTask();
    // Wait for the delay to pass
    Thread.sleep(delay);
    task.call();
    // Verify that the old container is deleted
    assertFalse(oldContainerDir.exists());
  }

  /**
   * Testing that invalid states (including QUASI_CLOSED in production mode) are correctly rejected.
   * Here, with QUASI_CLOSED state, we ensure that the test runs in production mode
   * where QUASI_CLOSED is not allowed for move.
   */
  @ParameterizedTest
  @EnumSource(names = {"OPEN", "CLOSING", "QUASI_CLOSED", "UNHEALTHY", "INVALID", "DELETED", "RECOVERING"})
  public void testMoveSkippedWhenContainerStateChanged(State invalidState)
      throws IOException, InterruptedException, TimeoutException {
    LogCapturer serviceLog = LogCapturer.captureLogs(DiskBalancerService.class);

    // Create a CLOSED container which will be selected by DefaultContainerChoosingPolicy
    Container container = createContainer(CONTAINER_ID, sourceVolume, State.CLOSED);
    long initialSourceUsed = sourceVolume.getCurrentUsage().getUsedSpace();
    long initialDestUsed = destVolume.getCurrentUsage().getUsedSpace();
    long initialDestCommitted = destVolume.getCommittedBytes();
    long initialSourceDelta = diskBalancerService.getDeltaSizes().get(sourceVolume) == null ?
        0L : diskBalancerService.getDeltaSizes().get(sourceVolume);
    String oldContainerPath = container.getContainerData().getContainerPath();

    // Verify temp container directory doesn't exist before task execution
    Path tempContainerDir = destVolume.getTmpDir().toPath()
        .resolve(DISK_BALANCER_DIR).resolve(String.valueOf(CONTAINER_ID));
    assertFalse(Files.exists(tempContainerDir));

    // Get the task (container is selected as CLOSED)
    DiskBalancerService.DiskBalancerTask task = getTask();
    assertNotNull(task);

    // Change container state to invalid state (OPEN or DELETED) before move process starts
    KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();
    containerData.setState(invalidState);

    // Execute the task - it should skip the move due to invalid state
    task.call();

    // Verify that move process was skipped
    GenericTestUtils.waitFor(() ->
            serviceLog.getOutput().contains("skipping move process") &&
            serviceLog.getOutput().contains(String.valueOf(CONTAINER_ID)) &&
            serviceLog.getOutput().contains(invalidState.toString()),
        100, 5000);

    // Verify container is still in the original location
    Container originalContainer = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(originalContainer);
    assertEquals(container, originalContainer);
    assertEquals(invalidState, originalContainer.getContainerState());
    assertEquals(sourceVolume, originalContainer.getContainerData().getVolume());
    assertTrue(new File(oldContainerPath).exists(), "Container should still exist in original location");

    // Verify no temp directory was created
    assertFalse(Files.exists(tempContainerDir), "Temp container directory should not be created");

    // Verify volume usage is unchanged
    assertEquals(initialSourceUsed, sourceVolume.getCurrentUsage().getUsedSpace());
    assertEquals(initialDestUsed, destVolume.getCurrentUsage().getUsedSpace());

    // Verify metrics show failure (since move was skipped)
    assertEquals(1, diskBalancerService.getMetrics().getFailureCount());
    assertEquals(0, diskBalancerService.getMetrics().getSuccessCount());
    assertEquals(0, diskBalancerService.getMetrics().getSuccessBytes());

    // Verify committed bytes are released
    assertEquals(initialDestCommitted, destVolume.getCommittedBytes());

    // Verify container is removed from in-progress set
    assertFalse(diskBalancerService.getInProgressContainers().contains(ContainerID.valueOf(CONTAINER_ID)));

    // Verify delta sizes are restored
    assertEquals(initialSourceDelta, diskBalancerService.getDeltaSizes().get(sourceVolume));
  }

  private KeyValueContainer createContainer(long containerId, HddsVolume vol, State state)
      throws IOException {
    KeyValueContainerData containerData = new KeyValueContainerData(
        containerId, ContainerLayoutVersion.FILE_PER_BLOCK, CONTAINER_SIZE,
        UUID.randomUUID().toString(), datanodeUuid);
    containerData.setState(state);
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

  private DiskBalancerService.DiskBalancerTask getTask() {
    return (DiskBalancerService.DiskBalancerTask) diskBalancerService.getTasks().poll();
  }

  private void setLayoutAndSchemaForTest(ContainerTestVersionInfo versionInfo) {
    String schemaVersion = versionInfo.getSchemaVersion();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
  }
}
