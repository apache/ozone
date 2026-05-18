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
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.getUnhealthyDataScanResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.fs.MockSpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.FaultInjector;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.BlockDeletingService;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.interfaces.ScanResult;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.statemachine.background.BlockDeletingTask;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The balancer thread holds a <em>read</em> lock on the old replica while it copies the
 * container and then calls {@link ContainerSet#updateContainer} so the map points at the new
 * replica on another disk. Concurrent delete / block-deletion / unhealthy / close paths resolve
 * the live container by id via {@link ContainerSet#getContainerWithWriteLock} and then operate on
 * that instance so paths, DB, and state match the destination replica.
 */
@Timeout(60)
class TestDiskBalancerWithConcurrentBackgroundTasks {

  @TempDir
  private java.nio.file.Path tmpDir;

  private File testDir;
  private final String scmId = UUID.randomUUID().toString();
  private final String datanodeUuid = UUID.randomUUID().toString();
  private final OzoneConfiguration conf = new OzoneConfiguration();

  private OzoneContainer ozoneContainer;
  private ContainerSet containerSet;
  private MutableVolumeSet volumeSet;
  private KeyValueHandler keyValueHandler;
  private ContainerChecksumTreeManager checksumTreeManager;
  private DiskBalancerServiceTestImpl diskBalancerService;

  private HddsVolume hotVolume;
  private HddsVolume coldVolume;

  private static final long CONTAINER_ID = 42L;
  private static final long CONTAINER_SIZE = 1024L * 1024L;
  private static final long SEEDED_PENDING_BLOCKS = 2L;
  private static final long SEEDED_PENDING_BYTES = 2048L;

  /**
   * Pauses disk balancer immediately after {@link ContainerSet#updateContainer} (map points at
   * the new replica) but before readUnlock, so another thread can run while the balancer
   * still holds readLock on the old container.
   */
  private static final class AfterInMemoryUpdateInjector extends FaultInjector {
    private final CountDownLatch reachedSwapPoint = new CountDownLatch(1);
    private final CountDownLatch continueBalancer = new CountDownLatch(1);

    @Override
    public void pause() throws IOException {
      // Signal the test thread that the race window has started, then block the balancer.
      reachedSwapPoint.countDown();
      try {
        continueBalancer.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    void awaitSwapPoint() throws InterruptedException, TimeoutException {
      if (!reachedSwapPoint.await(60, TimeUnit.SECONDS)) {
        throw new TimeoutException("balancer did not reach post-updateContainer hook");
      }
    }

    // Unblock the balancer so it can readUnlock and finish the move.
    void continueBalancer() {
      continueBalancer.countDown();
    }
  }

  @BeforeEach
  void setup() throws Exception {
    testDir = tmpDir.toFile();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());

    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        testDir.getAbsolutePath() + "/vol0,"
            + testDir.getAbsolutePath() + "/vol1,"
            + testDir.getAbsolutePath() + "/vol2");
    conf.setClass(SpaceUsageCheckFactory.Conf.configKeyForClassName(),
        MockSpaceUsageCheckFactory.HalfTera.class,
        SpaceUsageCheckFactory.class);

    volumeSet = new MutableVolumeSet(datanodeUuid, scmId, conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    createDbInstancesForTestIfNeeded(volumeSet, scmId, scmId, conf);

    List<StorageVolume> volumes = volumeSet.getVolumesList();
    HddsVolume v0 = (HddsVolume) volumes.get(0);
    HddsVolume v1 = (HddsVolume) volumes.get(1);
    HddsVolume v2 = (HddsVolume) volumes.get(2);

    for (HddsVolume v : new HddsVolume[] {v0, v1, v2}) {
      v.incrementUsedSpace(0 - v.getCurrentUsage().getUsedSpace());
    }

    long capacity = v0.getCurrentUsage().getCapacity();
    v0.incrementUsedSpace(capacity / 20);
    v1.incrementUsedSpace(capacity / 20);
    v2.incrementUsedSpace(capacity / 2);

    coldVolume = coldestVolume(v0, v1, v2);
    hotVolume = hottestVolume(v0, v1, v2);

    containerSet = ContainerSet.newReadOnlyContainerSet(1000);
    ContainerMetrics containerMetrics = ContainerMetrics.create(conf);
    checksumTreeManager = new ContainerChecksumTreeManager(conf);
    keyValueHandler = new KeyValueHandler(conf, datanodeUuid,
        containerSet, volumeSet, containerMetrics, c -> { },
        checksumTreeManager);
    keyValueHandler.setClusterID(scmId);

    Map<ContainerProtos.ContainerType, Handler> handlers = new HashMap<>();
    handlers.put(ContainerProtos.ContainerType.KeyValueContainer, keyValueHandler);
    ContainerController controller = new ContainerController(containerSet, handlers);
    ContainerDispatcher dispatcher = mock(ContainerDispatcher.class);
    when(dispatcher.getHandler(ContainerProtos.ContainerType.KeyValueContainer))
        .thenReturn(keyValueHandler);

    ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
    when(ozoneContainer.getVolumeSet()).thenReturn(volumeSet);
    when(ozoneContainer.getController()).thenReturn(controller);
    when(ozoneContainer.getDispatcher()).thenReturn(dispatcher);

    DiskBalancerConfiguration diskBalancerConfiguration = conf.getObject(DiskBalancerConfiguration.class);
    diskBalancerConfiguration.setDiskBalancerShouldRun(true);
    conf.setFromObject(diskBalancerConfiguration);
    diskBalancerService = new DiskBalancerServiceTestImpl(ozoneContainer, 100, conf, 1);
    // Immediate cleanup of the source replica after a move
    diskBalancerService.setReplicaDeletionDelay(0);
  }

  @AfterEach
  void cleanup() throws IOException {
    DiskBalancerService.setInjector(null);
    if (diskBalancerService != null) {
      diskBalancerService.shutdown();
    }
    BlockUtils.shutdownCache(conf);
    if (volumeSet != null) {
      volumeSet.shutdown();
    }
    if (testDir != null && testDir.exists()) {
      FileUtils.deleteDirectory(testDir);
    }
  }

  /**
   * Force-delete with a stale {@link Container} handle still targets the container id; after a
   * DiskBalancer swap, {@code deleteInternal} locks the live map entry (destination replica) and
   * applies deletion there — not on the old source object passed in from the RPC.
   */
  @ContainerTestVersionInfo.ContainerTest
  void containerDeleteStaleRefKeepsSwappedReplica(ContainerTestVersionInfo versionInfo)
      throws Exception {
    // Capture delete path logs: deleteInternal and diskBalancer
    LogCapturer kvLogs = LogCapturer.captureLogs(KeyValueHandler.class);
    LogCapturer diskBalancerLogs = LogCapturer.captureLogs(DiskBalancerService.class);

    String schemaVersion = versionInfo.getSchemaVersion();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);

    KeyValueContainer oldReplica = createClosedContainer(CONTAINER_ID, hotVolume, versionInfo);
    Container staleContainerRef = oldReplica;
    String oldReplicaPathOnHot = oldReplica.getContainerData().getContainerPath();
    assertThat(new File(oldReplicaPathOnHot)).exists();

    // Install injector so balancer pauses right after ContainerSet points at the new location of replica.
    AfterInMemoryUpdateInjector raceInjector = new AfterInMemoryUpdateInjector();
    DiskBalancerService.setInjector(raceInjector);

    DiskBalancerService.DiskBalancerTask task =
        (DiskBalancerService.DiskBalancerTask) diskBalancerService.getTasks().poll();
    assertNotNull(task);

    // Run the move on a background thread; it will block inside the injector.
    CompletableFuture<Void> balancerDone =
        CompletableFuture.runAsync(() -> task.call());

    // Wait until updateContainer(newReplica) is done; balancer still holds readLock on old replica.
    raceInjector.awaitSwapPoint();

    Container<?> currentContainerRef = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(currentContainerRef);
    assertNotEquals(staleContainerRef, currentContainerRef,
        "ContainerSet should reference the new replica before readUnlock");
    assertEquals(coldVolume, currentContainerRef.getContainerData().getVolume(),
        "Replica should already be on the destination volume");

    String destinationPathBeforeDelete =
        currentContainerRef.getContainerData().getContainerPath();

    // Start RM-style force delete using the stale Container handle; deleteInternal resolves id 42,
    // acquires writeLock on the live map entry (destination replica), not on the stale source handle.
    CountDownLatch deleteThreadPastSchedule = new CountDownLatch(1);
    CompletableFuture<Void> deleteDone = CompletableFuture.runAsync(() -> {
      deleteThreadPastSchedule.countDown();
      try {
        keyValueHandler.deleteContainer(staleContainerRef, true);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    assertThat(deleteThreadPastSchedule.await(10, TimeUnit.SECONDS)).isTrue();
    Thread.sleep(200);

    raceInjector.continueBalancer();

    CompletableFuture.allOf(balancerDone, deleteDone).get(60, TimeUnit.SECONDS);

    assertThat(kvLogs.getOutput()).doesNotContain("reference is stale");

    // Old replica: disk balancer marks it DELETED after the move.
    assertEquals(State.DELETED, staleContainerRef.getContainerState());
    assertEquals(hotVolume, staleContainerRef.getContainerData().getVolume());

    // Live map entry was removed by force-delete on the destination replica.
    assertNull(containerSet.getContainer(CONTAINER_ID));
    assertThat(new File(destinationPathBeforeDelete)).doesNotExist();

    // Disk balancer delayed cleanup removes the old replica from the source path — not RM delete.
    assertThat(new File(oldReplicaPathOnHot)).doesNotExist();
    GenericTestUtils.waitFor(
        () -> diskBalancerLogs.getOutput().contains("Deleted expired container 42 after delay")
            && diskBalancerLogs.getOutput().contains(String.valueOf(CONTAINER_ID)),
        100, 10_000);
  }

  /**
   * BlockDeletingTask queued with stale ref of KeyValueContainerData still resolves
   * the live replica by id; after {@code getContainerWithWriteLock}, it uses the destination
   * replica's DB and paths (updated {@code containerData} field).
   */
  @ContainerTestVersionInfo.ContainerTest
  void blockTaskStaleDataKeepsPendingOnDestination(ContainerTestVersionInfo versionInfo)
      throws Exception {
    LogCapturer logs = LogCapturer.captureLogs(BlockDeletingTask.class);
    LogCapturer diskBalancerLogs = LogCapturer.captureLogs(DiskBalancerService.class);
    String schemaVersion = versionInfo.getSchemaVersion();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);

    KeyValueContainer oldReplica = createClosedContainer(CONTAINER_ID, hotVolume, versionInfo);
    seedPendingDeletionInMetadata(oldReplica);
    KeyValueContainerData staleReplicaData = oldReplica.getContainerData();

    // Balancer injector — pause after map swap, before readUnlock.
    AfterInMemoryUpdateInjector raceInjector = new AfterInMemoryUpdateInjector();
    DiskBalancerService.setInjector(raceInjector);

    DiskBalancerService.DiskBalancerTask balancerTask =
        (DiskBalancerService.DiskBalancerTask) diskBalancerService.getTasks().poll();
    assertNotNull(balancerTask);
    CompletableFuture<Void> balancerDone =
        CompletableFuture.runAsync(() -> balancerTask.call());

    // We are past updateContainer; new replica is in ContainerSet; balancer still read-locks old replica.
    raceInjector.awaitSwapPoint();

    // New ContainerData instance must differ from what the queued block task still holds.
    Container<?> newReplicaData = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(newReplicaData);
    assertNotEquals(staleReplicaData, newReplicaData.getContainerData());
    KeyValueContainerData newData = (KeyValueContainerData) newReplicaData.getContainerData();
    long pendingBefore = readPendingDeleteBlockCount(newData);
    assertEquals(pendingBefore, SEEDED_PENDING_BLOCKS,
        "new replica should report same pending deletions from copied metadata");

    BlockDeletingService blockDeletingService =
        new BlockDeletingService(ozoneContainer, 500, 500, TimeUnit.MILLISECONDS, 1, conf,
            checksumTreeManager);
    BlockDeletingService.ContainerBlockInfo blockInfo =
        new BlockDeletingService.ContainerBlockInfo(staleReplicaData, SEEDED_PENDING_BLOCKS + 10);
    BlockDeletingTask blockDeletingTask =
        new BlockDeletingTask(blockDeletingService, blockInfo, checksumTreeManager, 1);

    // Run block deletion concurrently; getContainerWithWriteLock targets the live map entry on the destination.
    CountDownLatch blockThreadStarted = new CountDownLatch(1);
    CompletableFuture<Void> blockDone = CompletableFuture.runAsync(() -> {
      blockThreadStarted.countDown();
      try {
        blockDeletingTask.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    assertThat(blockThreadStarted.await(10, TimeUnit.SECONDS)).isTrue();
    Thread.sleep(200);

    raceInjector.continueBalancer();
    CompletableFuture.allOf(balancerDone, blockDone).get(60, TimeUnit.SECONDS);

    assertThat(logs.getOutput()).doesNotContain("reference is stale");

    KeyValueContainerData newContainerData =
        (KeyValueContainerData) containerSet.getContainer(CONTAINER_ID).getContainerData();
    assertNotNull(newContainerData);
    assertThat(readPendingDeleteBlockCount(newContainerData)).isLessThanOrEqualTo(pendingBefore);
    assertThat(new File(newContainerData.getChunksPath())).exists();
    // Disk balancer delayed cleanup removes the old replica from the source path — not RM delete.
    assertThat(new File(staleReplicaData.getContainerPath())).doesNotExist();
    GenericTestUtils.waitFor(
        () -> diskBalancerLogs.getOutput().contains("Deleted expired container 42 after delay")
            && diskBalancerLogs.getOutput().contains(String.valueOf(CONTAINER_ID)),
        100, 10_000);
  }

  /**
   * {@link KeyValueHandler#markContainerUnhealthy} with a stale container
   * reference while DiskBalancer has already run {@link ContainerSet#updateContainer}
   * (map = destination). Without {@link ContainerSet#getContainerWithWriteLock}, the handler would
   * take writeLock on the old object and mark it unhealthy after {@code markContainerForDelete}
   * turns it {@link State#DELETED}, sending a false UNHEALTHY ICR for a replica SCM no longer tracks.
   * With the fix, the live map entry (destination) is locked and marked {@link State#UNHEALTHY}.
   */
  @ContainerTestVersionInfo.ContainerTest
  void markUnhealthyAppliedOnDestVolumeContainer(
      ContainerTestVersionInfo versionInfo) throws Exception {
    LogCapturer diskBalancerLogs = LogCapturer.captureLogs(DiskBalancerService.class);
    String schemaVersion = versionInfo.getSchemaVersion();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);

    KeyValueContainer oldReplica = createClosedContainer(CONTAINER_ID, hotVolume, versionInfo);
    Container staleContainerRef = oldReplica;

    AfterInMemoryUpdateInjector raceInjector = new AfterInMemoryUpdateInjector();
    DiskBalancerService.setInjector(raceInjector);

    DiskBalancerService.DiskBalancerTask balancerTask =
        (DiskBalancerService.DiskBalancerTask) diskBalancerService.getTasks().poll();
    assertNotNull(balancerTask);
    CompletableFuture<Void> balancerDone =
        CompletableFuture.runAsync(balancerTask::call);

    raceInjector.awaitSwapPoint();

    Container<?> liveReplica = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(liveReplica);
    assertNotEquals(staleContainerRef, liveReplica,
        "ContainerSet should reference the destination replica at the race hook");
    assertEquals(State.CLOSED, liveReplica.getContainerState());

    ScanResult reason = getUnhealthyDataScanResult();
    CountDownLatch unhealthyThreadStarted = new CountDownLatch(1);
    CompletableFuture<Void> unhealthyDone = CompletableFuture.runAsync(() -> {
      unhealthyThreadStarted.countDown();
      try {
        keyValueHandler.markContainerUnhealthy(staleContainerRef, reason);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    assertThat(unhealthyThreadStarted.await(10, TimeUnit.SECONDS)).isTrue();
    Thread.sleep(200);

    raceInjector.continueBalancer();
    CompletableFuture.allOf(balancerDone, unhealthyDone).get(60, TimeUnit.SECONDS);

    Container<?> afterMove = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(afterMove);
    assertSame(liveReplica, afterMove);
    assertEquals(State.UNHEALTHY, afterMove.getContainerState(),
        "UNHEALTHY must apply to the live destination replica, not the stale source handle");

    assertEquals(State.DELETED, staleContainerRef.getContainerState());
    assertEquals(hotVolume, staleContainerRef.getContainerData().getVolume());

    GenericTestUtils.waitFor(
        () -> diskBalancerLogs.getOutput().contains("Deleted expired container 42 after delay")
            && diskBalancerLogs.getOutput().contains(String.valueOf(CONTAINER_ID)),
        100, 10_000);
  }

  /**
   * SCM closeContainer with a stale source container while the map already references
   * the destination after {@link ContainerSet#updateContainer}. Without resolving the live instance,
   * the close would run on the source after it is DELETED and throw, leaving the destination
   * {@link State#QUASI_CLOSED}. With {@link ContainerSet#getContainerWithWriteLock}, the destination
   * is closed to {@link State#CLOSED}.
   */
  @ContainerTestVersionInfo.ContainerTest
  void closeContainerAppliesOnDestVolumeContainer(
      ContainerTestVersionInfo versionInfo) throws Exception {
    LogCapturer diskBalancerLogs = LogCapturer.captureLogs(DiskBalancerService.class);
    String schemaVersion = versionInfo.getSchemaVersion();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);

    KeyValueContainer oldReplica = createClosedContainer(CONTAINER_ID, hotVolume, versionInfo);
    persistQuasiClosedState(oldReplica);
    Container staleContainerRef = oldReplica;

    AfterInMemoryUpdateInjector raceInjector = new AfterInMemoryUpdateInjector();
    DiskBalancerService.setInjector(raceInjector);

    DiskBalancerService.DiskBalancerTask balancerTask =
        (DiskBalancerService.DiskBalancerTask) diskBalancerService.getTasks().poll();
    assertNotNull(balancerTask);
    CompletableFuture<Void> balancerDone =
        CompletableFuture.runAsync(balancerTask::call);

    raceInjector.awaitSwapPoint();

    Container<?> liveReplica = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(liveReplica);
    assertNotEquals(staleContainerRef, liveReplica);
    assertEquals(State.QUASI_CLOSED, liveReplica.getContainerState());

    CountDownLatch closeThreadStarted = new CountDownLatch(1);
    CompletableFuture<Void> closeDone = CompletableFuture.runAsync(() -> {
      closeThreadStarted.countDown();
      try {
        keyValueHandler.closeContainer(staleContainerRef);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    assertThat(closeThreadStarted.await(10, TimeUnit.SECONDS)).isTrue();
    Thread.sleep(200);

    raceInjector.continueBalancer();
    CompletableFuture.allOf(balancerDone, closeDone).get(60, TimeUnit.SECONDS);

    Container<?> afterMove = containerSet.getContainer(CONTAINER_ID);
    assertNotNull(afterMove);
    assertSame(liveReplica, afterMove);
    assertEquals(State.CLOSED, afterMove.getContainerState(),
        "CLOSED transition must apply to the live destination replica");

    assertEquals(State.DELETED, staleContainerRef.getContainerState());

    GenericTestUtils.waitFor(
        () -> diskBalancerLogs.getOutput().contains("Deleted expired container 42 after delay")
            && diskBalancerLogs.getOutput().contains(String.valueOf(CONTAINER_ID)),
        100, 10_000);
  }

  /**
   * Makes {@link State#QUASI_CLOSED} visible on disk so import/copy sees the same state
   * the in-memory container had before the move.
   */
  private void persistQuasiClosedState(KeyValueContainer container) throws IOException {
    KeyValueContainerData data = container.getContainerData();
    data.setState(State.QUASI_CLOSED);
    File containerFile = ContainerUtils.getContainerFile(new File(data.getContainerPath()));
    ContainerDataYaml.createContainerFile(data, containerFile);
  }

  private long readPendingDeleteBlockCount(KeyValueContainerData data) throws IOException {
    try (DBHandle db = BlockUtils.getDB(data, conf)) {
      Table<String, Long> meta = db.getStore().getMetadataTable();
      Long v = meta.get(data.getPendingDeleteBlockCountKey());
      return v == null ? 0L : v;
    }
  }

  /**
   * Persists pending-deletion counters in container metadata so they survive
   * disk balancer copy/import and can be verified on the destination replica.
   */
  private void seedPendingDeletionInMetadata(KeyValueContainer container) throws IOException {
    KeyValueContainerData data = container.getContainerData();
    try (DBHandle metadata = BlockUtils.getDB(data, conf)) {
      Table<String, Long> meta = metadata.getStore().getMetadataTable();
      meta.put(data.getPendingDeleteBlockCountKey(), SEEDED_PENDING_BLOCKS);
      meta.put(data.getPendingDeleteBlockBytesKey(), SEEDED_PENDING_BYTES);
    }
    data.incrPendingDeletionBlocks(SEEDED_PENDING_BLOCKS, SEEDED_PENDING_BYTES);
  }

  private static HddsVolume coldestVolume(HddsVolume... volumes) {
    return Arrays.stream(volumes)
        .min(volumePolicyOrder())
        .get();
  }

  private static HddsVolume hottestVolume(HddsVolume... volumes) {
    return Arrays.stream(volumes)
        .max(volumePolicyOrder())
        .get();
  }

  private static Comparator<HddsVolume> volumePolicyOrder() {
    return Comparator
        .comparingDouble((HddsVolume v) -> {
          SpaceUsageSource usage = v.getCurrentUsage();
          return (double) (usage.getCapacity() - usage.getAvailable()) / usage.getCapacity();
        })
        .thenComparing(HddsVolume::getStorageID);
  }

  private KeyValueContainer createClosedContainer(long containerId, HddsVolume vol,
      ContainerTestVersionInfo versionInfo)
      throws IOException {
    KeyValueContainerData containerData = new KeyValueContainerData(
        containerId, versionInfo.getLayout(), CONTAINER_SIZE,
        UUID.randomUUID().toString(), datanodeUuid);
    containerData.setState(State.CLOSED);
    containerData.getStatistics().setBlockBytesForTesting(CONTAINER_SIZE);
    containerData.setSchemaVersion(versionInfo.getSchemaVersion());

    KeyValueContainer container = new KeyValueContainer(containerData, conf);
    VolumeChoosingPolicy policy = mock(VolumeChoosingPolicy.class);
    when(policy.chooseVolume(any(List.class), anyLong())).thenReturn(vol);
    container.create((VolumeSet) volumeSet, policy, scmId);
    containerSet.addContainer(container);
    vol.incrementUsedSpace(containerData.getBytesUsed());
    return container;
  }
}
