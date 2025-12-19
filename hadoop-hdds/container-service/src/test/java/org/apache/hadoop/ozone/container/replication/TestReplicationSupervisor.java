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

package org.apache.hadoop.ozone.container.replication;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority.LOW;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority.NORMAL;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.apache.hadoop.ozone.container.replication.AbstractReplicationTask.Status.DONE;
import static org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand.fromSources;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ProtoUtils;
import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.checksum.ReconcileContainerTask;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeChoosingPolicyFactory;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCommandInfo;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinator;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinatorTask;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionMetrics;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test the replication supervisor.
 */
public class TestReplicationSupervisor {

  private static final long CURRENT_TERM = 1;

  @TempDir
  private File tempDir;

  private final ContainerReplicator noopReplicator = task -> { };
  private final ContainerReplicator throwingReplicator = task -> {
    throw new RuntimeException("testing replication failure");
  };
  private final ContainerReplicator slowReplicator = task -> {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }
  };
  private final AtomicReference<ContainerReplicator> replicatorRef =
      new AtomicReference<>();
  private final AtomicReference<ECReconstructionCoordinator> ecReplicatorRef =
      new AtomicReference<>();

  private ContainerSet set;

  private ContainerLayoutVersion layoutVersion;

  private StateContext context;
  private TestClock clock;
  private DatanodeDetails datanode;
  private DNContainerOperationClient mockClient;
  private ContainerController mockController;

  private VolumeChoosingPolicy volumeChoosingPolicy;

  @BeforeEach
  public void setUp() throws Exception {
    clock = new TestClock(Instant.now(), ZoneId.systemDefault());
    set = newContainerSet();
    DatanodeStateMachine stateMachine = mock(DatanodeStateMachine.class);
    context = new StateContext(
        new OzoneConfiguration(),
        DatanodeStateMachine.DatanodeStates.getInitState(),
        stateMachine, "");
    context.setTermOfLeaderSCM(CURRENT_TERM);
    datanode = MockDatanodeDetails.randomDatanodeDetails();
    mockClient = mock(DNContainerOperationClient.class);
    mockController = mock(ContainerController.class);
    when(stateMachine.getDatanodeDetails()).thenReturn(datanode);
    volumeChoosingPolicy = VolumeChoosingPolicyFactory.getPolicy(new OzoneConfiguration());
  }

  @AfterEach
  public void cleanup() {
    replicatorRef.set(null);
    ecReplicatorRef.set(null);
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void normal(ContainerLayoutVersion layout) {
    this.layoutVersion = layout;
    // GIVEN
    ReplicationSupervisor supervisor =
        supervisorWithReplicator(FakeReplicator::new);
    ReplicationSupervisorMetrics metrics =
        ReplicationSupervisorMetrics.create(supervisor);

    try {
      //WHEN
      supervisor.addTask(createTask(1L));
      supervisor.addTask(createTask(2L));
      supervisor.addTask(createTask(5L));

      assertEquals(3, supervisor.getReplicationRequestCount());
      assertEquals(3, supervisor.getReplicationSuccessCount());
      assertEquals(0, supervisor.getReplicationFailureCount());
      assertEquals(0, supervisor.getTotalInFlightReplications());
      assertEquals(0, supervisor.getQueueSize());
      assertEquals(3, set.containerCount());

      MetricsCollectorImpl metricsCollector = new MetricsCollectorImpl();
      metrics.getMetrics(metricsCollector, true);
      assertEquals(1, metricsCollector.getRecords().size());
    } finally {
      metrics.unRegister();
      supervisor.stop();
    }
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void duplicateMessage(ContainerLayoutVersion layout) {
    this.layoutVersion = layout;
    // GIVEN
    ReplicationSupervisor supervisor = supervisorWithReplicator(
        FakeReplicator::new);

    try {
      //WHEN
      supervisor.addTask(createTask(6L));
      supervisor.addTask(createTask(6L));
      supervisor.addTask(createTask(6L));
      supervisor.addTask(createTask(6L));

      //THEN
      assertEquals(4, supervisor.getReplicationRequestCount());
      assertEquals(1, supervisor.getReplicationSuccessCount());
      assertEquals(0, supervisor.getReplicationFailureCount());
      assertEquals(3, supervisor.getReplicationSkippedCount());
      assertEquals(0, supervisor.getTotalInFlightReplications());
      assertEquals(0, supervisor.getQueueSize());
      assertEquals(1, set.containerCount());
    } finally {
      supervisor.stop();
    }
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void failureHandling(ContainerLayoutVersion layout) {
    this.layoutVersion = layout;
    // GIVEN
    ReplicationSupervisor supervisor = supervisorWith(
        __ -> throwingReplicator, newDirectExecutorService());

    try {
      //WHEN
      ReplicationTask task = createTask(1L);
      supervisor.addTask(task);

      //THEN
      assertEquals(1, supervisor.getReplicationRequestCount());
      assertEquals(0, supervisor.getReplicationSuccessCount());
      assertEquals(1, supervisor.getReplicationFailureCount());
      assertEquals(0, supervisor.getTotalInFlightReplications());
      assertEquals(0, supervisor.getQueueSize());
      assertEquals(0, set.containerCount());
      assertEquals(ReplicationTask.Status.FAILED, task.getStatus());
    } finally {
      supervisor.stop();
    }
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void stalledDownload() {
    // GIVEN
    ReplicationSupervisor supervisor = supervisorWith(__ -> noopReplicator,
        new DiscardingExecutorService());

    try {
      //WHEN
      supervisor.addTask(createTask(1L));
      supervisor.addTask(createTask(2L));
      supervisor.addTask(createTask(3L));
      supervisor.addTask(createECTask(4L));
      supervisor.addTask(createECTask(5L));

      //THEN
      assertEquals(0, supervisor.getReplicationRequestCount());
      assertEquals(0, supervisor.getReplicationSuccessCount());
      assertEquals(0, supervisor.getReplicationFailureCount());
      assertEquals(5, supervisor.getTotalInFlightReplications());
      assertEquals(3, supervisor.getInFlightReplications(
          ReplicationTask.class));
      assertEquals(2, supervisor.getInFlightReplications(
          ECReconstructionCoordinatorTask.class));
      assertEquals(0, supervisor.getQueueSize());
      assertEquals(0, set.containerCount());
    } finally {
      supervisor.stop();
    }
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void slowDownload() {
    // GIVEN
    ReplicationSupervisor supervisor = supervisorWith(__ -> slowReplicator,
        new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>()));

    try {
      //WHEN
      supervisor.addTask(createTask(1L));
      supervisor.addTask(createTask(2L));
      supervisor.addTask(createTask(3L));

      //THEN
      assertEquals(3, supervisor.getTotalInFlightReplications());
      assertEquals(2, supervisor.getQueueSize());
      // Sleep 4s, wait all tasks processed
      try {
        Thread.sleep(4000);
      } catch (InterruptedException e) {
      }
      assertEquals(0, supervisor.getTotalInFlightReplications());
      assertEquals(0, supervisor.getQueueSize());
    } finally {
      supervisor.stop();
    }
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testDownloadAndImportReplicatorFailure(ContainerLayoutVersion layout,
      @TempDir File tempFile) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();

    ReplicationSupervisor supervisor = ReplicationSupervisor.newBuilder()
        .stateContext(context)
        .executor(newDirectExecutorService())
        .clock(clock)
        .build();

    // Mock to fetch an exception in the importContainer method.
    SimpleContainerDownloader moc =
        mock(SimpleContainerDownloader.class);
    Path res = Paths.get("file:/tmp/no-such-file");
    when(
        moc.getContainerDataFromReplicas(anyLong(), anyList(),
            any(Path.class), any()))
        .thenReturn(res);

    final String testDir = tempFile.getPath();
    MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);
    when(volumeSet.getVolumesList())
        .thenReturn(singletonList(
            new HddsVolume.Builder(testDir).conf(conf).build()));
    ContainerController mockedCC =
        mock(ContainerController.class);
    ContainerImporter importer =
        new ContainerImporter(conf, set, mockedCC, volumeSet, volumeChoosingPolicy);
    ContainerReplicator replicator =
        new DownloadAndImportReplicator(conf, set, importer, moc);

    replicatorRef.set(replicator);

    LogCapturer logCapturer = LogCapturer.captureLogs(DownloadAndImportReplicator.class);

    supervisor.addTask(createTask(1L));
    assertEquals(1, supervisor.getReplicationFailureCount());
    assertEquals(0, supervisor.getReplicationSuccessCount());
    assertThat(logCapturer.getOutput())
        .contains("Container 1 replication was unsuccessful.");
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testReplicationImportReserveSpace(ContainerLayoutVersion layout)
      throws IOException, InterruptedException, TimeoutException {
    final long containerUsedSize = 100;
    this.layoutVersion = layout;
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, tempDir.getAbsolutePath());

    long containerMaxSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);

    ReplicationSupervisor supervisor = ReplicationSupervisor.newBuilder()
        .stateContext(context)
        .executor(newDirectExecutorService())
        .clock(clock)
        .build();

    MutableVolumeSet volumeSet = new MutableVolumeSet(datanode.getUuidString(), conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);

    long containerId = 1;
    // create container
    KeyValueContainerData containerData = new KeyValueContainerData(containerId,
        ContainerLayoutVersion.FILE_PER_BLOCK, containerMaxSize, "test", "test");
    HddsVolume vol1 = (HddsVolume) volumeSet.getVolumesList().get(0);
    containerData.setVolume(vol1);
    // the container is not yet in HDDS, so only set its own size, leaving HddsVolume with used=0
    containerData.getStatistics().updateWrite(100, false);
    KeyValueContainer container = new KeyValueContainer(containerData, conf);
    ContainerController controllerMock = mock(ContainerController.class);
    Semaphore semaphore = new Semaphore(1);
    when(controllerMock.importContainer(any(), any(), any()))
        .thenAnswer((invocation) -> {
          semaphore.acquire();
          return container;
        });
    
    File tarFile = containerTarFile(containerId, containerData);

    SimpleContainerDownloader moc =
        mock(SimpleContainerDownloader.class);
    when(
        moc.getContainerDataFromReplicas(anyLong(), anyList(),
            any(Path.class), any()))
        .thenReturn(tarFile.toPath());

    ContainerImporter importer =
        new ContainerImporter(conf, set, controllerMock, volumeSet, volumeChoosingPolicy);

    // Initially volume has 0 commit space
    assertEquals(0, vol1.getCommittedBytes());
    long usedSpace = vol1.getCurrentUsage().getUsedSpace();
    // Initially volume has 0 used space
    assertEquals(0, usedSpace);
    // Increase committed bytes so that volume has only remaining 3 times container size space
    long minFreeSpace =
        conf.getObject(DatanodeConfiguration.class).getMinFreeSpace(vol1.getCurrentUsage().getCapacity());
    long initialCommittedBytes = vol1.getCurrentUsage().getCapacity() - containerMaxSize * 3 - minFreeSpace;
    vol1.incCommittedBytes(initialCommittedBytes);
    ContainerReplicator replicator =
        new DownloadAndImportReplicator(conf, set, importer, moc);
    replicatorRef.set(replicator);

    LogCapturer logCapturer = LogCapturer.captureLogs(DownloadAndImportReplicator.class);

    // Acquire semaphore so that container import will pause after reserving space.
    semaphore.acquire();
    CompletableFuture.runAsync(() -> {
      try {
        supervisor.addTask(createTask(containerId));
      } catch (Exception ex) {
      }
    });

    // Wait such that first container import reserve space
    GenericTestUtils.waitFor(() ->
        vol1.getCommittedBytes() > initialCommittedBytes,
        1000, 50000);

    // Volume has reserved space of 2 * containerSize
    assertEquals(vol1.getCommittedBytes(), initialCommittedBytes + 2 * containerMaxSize);
    // Container 2 import will fail as container 1 has reserved space and no space left to import new container
    // New container import requires at least (2 * container size)
    long containerId2 = 2;
    supervisor.addTask(createTask(containerId2));
    GenericTestUtils.waitFor(() -> 1 == supervisor.getReplicationFailureCount(),
        1000, 50000);
    assertThat(logCapturer.getOutput()).contains("No volumes have enough space for a new container");
    // Release semaphore so that first container import will pass
    semaphore.release();
    GenericTestUtils.waitFor(() ->
        1 == supervisor.getReplicationSuccessCount(), 1000, 50000);

    usedSpace = vol1.getCurrentUsage().getUsedSpace();
    // After replication, volume used space should be increased by container used bytes
    assertEquals(containerUsedSize, usedSpace);

    // Volume committed bytes used for replication has been released, no need to reserve space for imported container
    // only closed container gets replicated, so no new data will be written it
    assertEquals(vol1.getCommittedBytes(), initialCommittedBytes);

  }

  private File containerTarFile(
      long containerId, ContainerData containerData) throws IOException {
    File yamlFile = new File(tempDir, "container.yaml");
    ContainerDataYaml.createContainerFile(containerData,
        yamlFile);
    File tarFile = new File(tempDir,
        ContainerUtils.getContainerTarName(containerId));
    try (OutputStream output = Files.newOutputStream(tarFile.toPath())) {
      ArchiveOutputStream<TarArchiveEntry> archive = new TarArchiveOutputStream(output);
      TarArchiveEntry entry = archive.createArchiveEntry(yamlFile,
          "container.yaml");
      archive.putArchiveEntry(entry);
      try (InputStream input = Files.newInputStream(yamlFile.toPath())) {
        IOUtils.copy(input, archive);
      }
      archive.closeArchiveEntry();
    }
    return tarFile;
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testTaskBeyondDeadline(ContainerLayoutVersion layout) {
    this.layoutVersion = layout;
    ReplicationSupervisor supervisor =
        supervisorWithReplicator(FakeReplicator::new);

    ReplicateContainerCommand cmd = createCommand(1);
    cmd.setDeadline(clock.millis() + 10000);
    ReplicationTask task1 = new ReplicationTask(cmd, replicatorRef.get());
    cmd = createCommand(2);
    cmd.setDeadline(clock.millis() + 20000);
    ReplicationTask task2 = new ReplicationTask(cmd, replicatorRef.get());
    cmd = createCommand(3);
    // No deadline set
    ReplicationTask task3 = new ReplicationTask(cmd, replicatorRef.get());
    // no deadline set

    clock.fastForward(15000);

    supervisor.addTask(task1);
    supervisor.addTask(task2);
    supervisor.addTask(task3);

    assertEquals(3, supervisor.getReplicationRequestCount());
    assertEquals(2, supervisor.getReplicationSuccessCount());
    assertEquals(0, supervisor.getReplicationFailureCount());
    assertEquals(0, supervisor.getTotalInFlightReplications());
    assertEquals(0, supervisor.getQueueSize());
    assertEquals(1, supervisor.getReplicationTimeoutCount());
    assertEquals(2, set.containerCount());

  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testDatanodeOutOfService(ContainerLayoutVersion layout) {
    this.layoutVersion = layout;
    ReplicationSupervisor supervisor =
        supervisorWithReplicator(FakeReplicator::new);
    datanode.setPersistedOpState(
        HddsProtos.NodeOperationalState.DECOMMISSIONING);

    ReplicateContainerCommand pushCmd = ReplicateContainerCommand.toTarget(
        1, MockDatanodeDetails.randomDatanodeDetails());
    pushCmd.setTerm(CURRENT_TERM);
    ReplicateContainerCommand pullCmd = createCommand(2);

    supervisor.addTask(new ReplicationTask(pushCmd, replicatorRef.get()));
    supervisor.addTask(new ReplicationTask(pullCmd, replicatorRef.get()));

    assertEquals(2, supervisor.getReplicationRequestCount());
    assertEquals(1, supervisor.getReplicationSuccessCount());
    assertEquals(0, supervisor.getReplicationFailureCount());
    assertEquals(0, supervisor.getTotalInFlightReplications());
    assertEquals(0, supervisor.getQueueSize());
    assertEquals(0, supervisor.getReplicationTimeoutCount());
    assertEquals(1, set.containerCount());
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void taskWithObsoleteTermIsDropped(ContainerLayoutVersion layout) {
    this.layoutVersion = layout;
    final long newTerm = 2;
    ReplicationSupervisor supervisor =
        supervisorWithReplicator(FakeReplicator::new);

    context.setTermOfLeaderSCM(newTerm);
    supervisor.addTask(createTask(1L));

    assertEquals(1, supervisor.getReplicationRequestCount());
    assertEquals(0, supervisor.getReplicationSuccessCount());
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testMultipleReplication(ContainerLayoutVersion layout,
      @TempDir File tempFile) throws IOException {
    this.layoutVersion = layout;
    OzoneConfiguration conf = new OzoneConfiguration();
    // GIVEN
    ReplicationSupervisor replicationSupervisor =
        supervisorWithReplicator(FakeReplicator::new);
    ReplicationSupervisor ecReconstructionSupervisor = supervisorWithECReconstruction();
    ReplicationSupervisorMetrics replicationMetrics =
        ReplicationSupervisorMetrics.create(replicationSupervisor);
    ReplicationSupervisorMetrics ecReconstructionMetrics =
        ReplicationSupervisorMetrics.create(ecReconstructionSupervisor);
    try {
      //WHEN
      replicationSupervisor.addTask(createTask(1L));
      ecReconstructionSupervisor.addTask(createECTaskWithCoordinator(2L));
      replicationSupervisor.addTask(createTask(1L));
      replicationSupervisor.addTask(createTask(3L));
      ecReconstructionSupervisor.addTask(createECTaskWithCoordinator(4L));

      SimpleContainerDownloader moc = mock(SimpleContainerDownloader.class);
      Path res = Paths.get("file:/tmp/no-such-file");
      when(moc.getContainerDataFromReplicas(anyLong(), anyList(),
          any(Path.class), any())).thenReturn(res);

      final String testDir = tempFile.getPath();
      MutableVolumeSet volumeSet = mock(MutableVolumeSet.class);
      when(volumeSet.getVolumesList()).thenReturn(singletonList(
          new HddsVolume.Builder(testDir).conf(conf).build()));
      ContainerController mockedCC = mock(ContainerController.class);
      ContainerImporter importer = new ContainerImporter(conf, set, mockedCC, volumeSet, volumeChoosingPolicy);
      ContainerReplicator replicator = new DownloadAndImportReplicator(
          conf, set, importer, moc);
      replicatorRef.set(replicator);
      replicationSupervisor.addTask(createTask(5L));

      ReplicateContainerCommand cmd1 = createCommand(6L);
      cmd1.setDeadline(clock.millis() + 10000);
      ReplicationTask task1 = new ReplicationTask(cmd1, replicatorRef.get());
      clock.fastForward(15000);
      replicationSupervisor.addTask(task1);

      ReconstructECContainersCommand cmd2 = createReconstructionCmd(7L);
      cmd2.setDeadline(clock.millis() + 10000);
      ECReconstructionCoordinatorTask task2 = new ECReconstructionCoordinatorTask(
          ecReplicatorRef.get(), new ECReconstructionCommandInfo(cmd2));
      clock.fastForward(15000);
      ecReconstructionSupervisor.addTask(task2);
      ecReconstructionSupervisor.addTask(createECTask(8L));
      ecReconstructionSupervisor.addTask(createECTask(9L));

      //THEN
      assertEquals(2, replicationSupervisor.getReplicationSuccessCount());
      assertEquals(2, replicationSupervisor.getReplicationSuccessCount(
          task1.getMetricName()));
      assertEquals(1, replicationSupervisor.getReplicationFailureCount());
      assertEquals(1, replicationSupervisor.getReplicationFailureCount(
          task1.getMetricName()));
      assertEquals(1, replicationSupervisor.getReplicationSkippedCount());
      assertEquals(1, replicationSupervisor.getReplicationSkippedCount(
          task1.getMetricName()));
      assertEquals(1, replicationSupervisor.getReplicationTimeoutCount());
      assertEquals(1, replicationSupervisor.getReplicationTimeoutCount(
          task1.getMetricName()));
      assertEquals(5, replicationSupervisor.getReplicationRequestCount());
      assertEquals(5, replicationSupervisor.getReplicationRequestCount(
          task1.getMetricName()));
      assertEquals(0, replicationSupervisor.getReplicationRequestCount(
          task2.getMetricName()));

      assertEquals(2, ecReconstructionSupervisor.getReplicationSuccessCount());
      assertEquals(2, ecReconstructionSupervisor.getReplicationSuccessCount(
          task2.getMetricName()));
      assertEquals(1, ecReconstructionSupervisor.getReplicationTimeoutCount());
      assertEquals(1, ecReconstructionSupervisor.getReplicationTimeoutCount(
          task2.getMetricName()));
      assertEquals(2, ecReconstructionSupervisor.getReplicationFailureCount());
      assertEquals(2, ecReconstructionSupervisor.getReplicationFailureCount(
          task2.getMetricName()));
      assertEquals(5, ecReconstructionSupervisor.getReplicationRequestCount());
      assertEquals(5, ecReconstructionSupervisor.getReplicationRequestCount(
          task2.getMetricName()));
      assertEquals(0, ecReconstructionSupervisor.getReplicationRequestCount(
          task1.getMetricName()));

      assertTrue(replicationSupervisor.getReplicationRequestTotalTime(
          task1.getMetricName()) > 0);
      assertTrue(ecReconstructionSupervisor.getReplicationRequestTotalTime(
          task2.getMetricName()) > 0);
      assertTrue(replicationSupervisor.getReplicationRequestAvgTime(
          task1.getMetricName()) > 0);
      assertTrue(ecReconstructionSupervisor.getReplicationRequestAvgTime(
          task2.getMetricName()) > 0);

      MetricsCollectorImpl replicationMetricsCollector = new MetricsCollectorImpl();
      replicationMetrics.getMetrics(replicationMetricsCollector, true);
      assertEquals(1, replicationMetricsCollector.getRecords().size());

      MetricsCollectorImpl ecReconstructionMetricsCollector = new MetricsCollectorImpl();
      ecReconstructionMetrics.getMetrics(ecReconstructionMetricsCollector, true);
      assertEquals(1, ecReconstructionMetricsCollector.getRecords().size());
    } finally {
      replicationMetrics.unRegister();
      ecReconstructionMetrics.unRegister();
      replicationSupervisor.stop();
      ecReconstructionSupervisor.stop();
    }
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testReconciliationTaskMetrics(ContainerLayoutVersion layout) throws IOException {
    this.layoutVersion = layout;
    // GIVEN
    ReplicationSupervisor replicationSupervisor =
        supervisorWithReplicator(FakeReplicator::new);
    ReplicationSupervisorMetrics replicationMetrics =
        ReplicationSupervisorMetrics.create(replicationSupervisor);

    try {
      //WHEN
      replicationSupervisor.addTask(createReconciliationTask(1L));
      replicationSupervisor.addTask(createReconciliationTask(2L));

      ReconcileContainerTask reconciliationTask = createReconciliationTask(6L);
      clock.fastForward(15000);
      replicationSupervisor.addTask(reconciliationTask);
      doThrow(IOException.class).when(mockController).reconcileContainer(any(), anyLong(), any());
      replicationSupervisor.addTask(createReconciliationTask(7L));

      //THEN
      assertEquals(2, replicationSupervisor.getReplicationSuccessCount());

      assertEquals(2, replicationSupervisor.getReplicationSuccessCount(
          reconciliationTask.getMetricName()));
      assertEquals(1, replicationSupervisor.getReplicationFailureCount());
      assertEquals(1, replicationSupervisor.getReplicationFailureCount(
          reconciliationTask.getMetricName()));
      assertEquals(1, replicationSupervisor.getReplicationTimeoutCount());
      assertEquals(1, replicationSupervisor.getReplicationTimeoutCount(
          reconciliationTask.getMetricName()));
      assertEquals(4, replicationSupervisor.getReplicationRequestCount());
      assertEquals(4, replicationSupervisor.getReplicationRequestCount(
          reconciliationTask.getMetricName()));


      assertTrue(replicationSupervisor.getReplicationRequestTotalTime(
          reconciliationTask.getMetricName()) > 0);
      assertTrue(replicationSupervisor.getReplicationRequestAvgTime(
          reconciliationTask.getMetricName()) > 0);

      MetricsCollectorImpl replicationMetricsCollector = new MetricsCollectorImpl();
      replicationMetrics.getMetrics(replicationMetricsCollector, true);
      assertEquals(1, replicationMetricsCollector.getRecords().size());
    } finally {
      replicationMetrics.unRegister();
      replicationSupervisor.stop();
    }
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testPriorityOrdering(ContainerLayoutVersion layout)
      throws InterruptedException {
    this.layoutVersion = layout;
    long deadline = clock.millis() + 1000;
    long containerId = 1;
    long term = 1;
    OzoneConfiguration conf = new OzoneConfiguration();
    ReplicationServer.ReplicationConfig repConf =
        conf.getObject(ReplicationServer.ReplicationConfig.class);
    repConf.setReplicationMaxStreams(1);
    ReplicationSupervisor supervisor = ReplicationSupervisor.newBuilder()
        .replicationConfig(repConf)
        .clock(clock)
        .build();

    final CountDownLatch indicateRunning = new CountDownLatch(1);
    final CountDownLatch completeRunning = new CountDownLatch(1);
    // Going to create 5 tasks below, so this counter needs to be set to 5.
    final CountDownLatch tasksCompleteLatch = new CountDownLatch(5);

    supervisor.addTask(new BlockingTask(containerId, deadline, term,
        indicateRunning, completeRunning));
    // Wait for the first task to block the single threaded executor
    indicateRunning.await();

    List<String> completionOrder = new ArrayList<>();
    // Now load some tasks out of order.
    clock.fastForward(10);
    supervisor.addTask(new OrderedTask(containerId, deadline, term, clock,
        LOW, "LOW_10", completionOrder, tasksCompleteLatch));
    clock.rewind(5);
    supervisor.addTask(new OrderedTask(containerId, deadline, term, clock,
        LOW, "LOW_5", completionOrder, tasksCompleteLatch));

    supervisor.addTask(new OrderedTask(containerId, deadline, term, clock,
        NORMAL, "HIGH_5", completionOrder, tasksCompleteLatch));
    clock.rewind(4);
    supervisor.addTask(new OrderedTask(containerId, deadline, term, clock,
        NORMAL, "HIGH_1", completionOrder, tasksCompleteLatch));
    clock.fastForward(10);
    supervisor.addTask(new OrderedTask(containerId, deadline, term, clock,
        NORMAL, "HIGH_11", completionOrder, tasksCompleteLatch));

    List<String> expectedOrder = new ArrayList<>();
    expectedOrder.add("HIGH_1");
    expectedOrder.add("HIGH_5");
    expectedOrder.add("HIGH_11");
    expectedOrder.add("LOW_5");
    expectedOrder.add("LOW_10");

    // Before unblocking the queue, check the queue count for the OrderedTask.
    // We loaded 3 High / normal priority and 2 low. The counter should not
    // include the low counts.
    assertEquals(3,
        supervisor.getInFlightReplications(OrderedTask.class));
    assertEquals(1,
        supervisor.getInFlightReplications(BlockingTask.class));

    // Unblock the queue
    completeRunning.countDown();
    // Wait for all tasks to complete
    tasksCompleteLatch.await();
    assertEquals(expectedOrder, completionOrder);
    assertEquals(0,
        supervisor.getInFlightReplications(OrderedTask.class));
    assertEquals(0,
        supervisor.getInFlightReplications(BlockingTask.class));
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testReconcileContainerCommandDeduplication() throws Exception {
    ReplicationSupervisor supervisor = ReplicationSupervisor.newBuilder()
        .stateContext(context)
        .build();

    try {
      final long containerID = 10L;

      // Create reconcile commands with the same container ID but different peers
      ReconcileContainerCommand command1 = new ReconcileContainerCommand(containerID, Collections.singleton(
          MockDatanodeDetails.randomDatanodeDetails()));
      command1.setTerm(1);
      ReconcileContainerCommand command2 = new ReconcileContainerCommand(containerID, Collections.singleton(
          MockDatanodeDetails.randomDatanodeDetails()));
      command2.setTerm(1);
      assertEquals(command1, command2);

      // Create a controller that blocks the execution of reconciliation until the latch is counted down from the test.
      ContainerController blockingController = mock(ContainerController.class);
      CountDownLatch latch = new CountDownLatch(1);
      doAnswer(arg -> {
        latch.await();
        return null;
      }).when(blockingController).reconcileContainer(any(), anyLong(), any());

      ReconcileContainerTask task1 = new ReconcileContainerTask(
          blockingController,
          mock(DNContainerOperationClient.class),
          command1);
      ReconcileContainerTask task2 = new ReconcileContainerTask(
          // The second task should be discarded as a duplicate. It does not need to block.
          mock(ContainerController.class),
          mock(DNContainerOperationClient.class),
          command2);

      // Add first task - should be accepted
      supervisor.addTask(task1);
      assertEquals(1, supervisor.getTotalInFlightReplications());
      assertEquals(1, supervisor.getReplicationQueuedCount());

      // Add second task with same container ID but different peers - should be deduplicated
      supervisor.addTask(task2);
      assertEquals(1, supervisor.getTotalInFlightReplications());
      assertEquals(1, supervisor.getReplicationQueuedCount());

      // Now the task has been unblocked. The supervisor should finish execution of the one blocked task.
      latch.countDown();
      GenericTestUtils.waitFor(() ->
          supervisor.getTotalInFlightReplications() == 0 && supervisor.getReplicationQueuedCount() == 0, 500, 5000);
    } finally {
      supervisor.stop();
    }
  }

  private static class BlockingTask extends AbstractReplicationTask {

    private final CountDownLatch runningLatch;
    private final CountDownLatch waitForCompleteLatch;

    BlockingTask(long containerId, long deadlineEpochMs, long term,
        CountDownLatch running, CountDownLatch waitForCompletion) {
      super(containerId, deadlineEpochMs, term);
      this.runningLatch = running;
      this.waitForCompleteLatch = waitForCompletion;
    }

    @Override
    protected String getMetricName() {
      return "Blockings";
    }

    @Override
    protected String getMetricDescriptionSegment() {
      return "blockings";
    }

    @Override
    public void runTask() {
      runningLatch.countDown();
      assertDoesNotThrow(() -> waitForCompleteLatch.await(),
          "Interrupted waiting for the completion latch to be released");
      setStatus(DONE);
    }
  }

  private static class OrderedTask extends  AbstractReplicationTask {

    private final String name;
    private final List<String> completeList;
    private final CountDownLatch completeLatch;

    @SuppressWarnings("checkstyle:parameterNumber")
    OrderedTask(long containerId, long deadlineEpochMs, long term,
        Clock clock, ReplicationCommandPriority priority,
        String name, List<String> completeList, CountDownLatch completeLatch) {
      super(containerId, deadlineEpochMs, term, clock);
      this.completeList = completeList;
      this.name = name;
      this.completeLatch = completeLatch;
      setPriority(priority);
    }

    @Override
    protected String getMetricName() {
      return "Ordereds";
    }

    @Override
    protected String getMetricDescriptionSegment() {
      return "ordereds";
    }

    @Override
    public void runTask() {
      completeList.add(name);
      setStatus(DONE);
      completeLatch.countDown();
    }
  }

  private ReplicationSupervisor supervisorWithReplicator(
      Function<ReplicationSupervisor, ContainerReplicator> replicatorFactory) {
    return supervisorWith(replicatorFactory, newDirectExecutorService());
  }

  private ReplicationSupervisor supervisorWith(
      Function<ReplicationSupervisor, ContainerReplicator> replicatorFactory,
      ExecutorService executor) {
    ConfigurationSource conf = new OzoneConfiguration();
    ReplicationServer.ReplicationConfig repConf =
        conf.getObject(ReplicationServer.ReplicationConfig.class);
    ReplicationSupervisor supervisor = ReplicationSupervisor.newBuilder()
        .stateContext(context)
        .replicationConfig(repConf)
        .executor(executor)
        .clock(clock)
        .build();
    replicatorRef.set(replicatorFactory.apply(supervisor));
    return supervisor;
  }

  private ReplicationSupervisor supervisorWithECReconstruction() throws IOException {
    ConfigurationSource conf = new OzoneConfiguration();
    ExecutorService executor = newDirectExecutorService();
    ReplicationServer.ReplicationConfig repConf =
        conf.getObject(ReplicationServer.ReplicationConfig.class);
    ReplicationSupervisor supervisor = ReplicationSupervisor.newBuilder()
        .stateContext(context).replicationConfig(repConf).executor(executor)
        .clock(clock).build();

    FakeECReconstructionCoordinator coordinator = new FakeECReconstructionCoordinator(
        new OzoneConfiguration(), null, null, context,
        ECReconstructionMetrics.create(), "", supervisor);
    ecReplicatorRef.set(coordinator);
    return supervisor;
  }

  private ReplicationTask createTask(long containerId) {
    ReplicateContainerCommand cmd = createCommand(containerId);
    return new ReplicationTask(cmd, replicatorRef.get());
  }

  private ReconcileContainerTask createReconciliationTask(long containerId) {
    ReconcileContainerCommand reconcileContainerCommand =
        new ReconcileContainerCommand(containerId, Collections.singleton(datanode));
    reconcileContainerCommand.setTerm(CURRENT_TERM);
    reconcileContainerCommand.setDeadline(clock.millis() + 10000);
    return new ReconcileContainerTask(mockController, mockClient,
        reconcileContainerCommand);
  }

  private ECReconstructionCoordinatorTask createECTask(long containerId) {
    return new ECReconstructionCoordinatorTask(null,
        createReconstructionCmdInfo(containerId));
  }

  private ECReconstructionCoordinatorTask createECTaskWithCoordinator(long containerId) {
    ECReconstructionCommandInfo ecReconstructionCommandInfo = createReconstructionCmdInfo(containerId);
    return new ECReconstructionCoordinatorTask(ecReplicatorRef.get(),
        ecReconstructionCommandInfo);
  }

  private static ReplicateContainerCommand createCommand(long containerId) {
    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.forTest(containerId);
    cmd.setTerm(CURRENT_TERM);
    return cmd;
  }

  private static ECReconstructionCommandInfo createReconstructionCmdInfo(
      long containerId) {
    return new ECReconstructionCommandInfo(createReconstructionCmd(containerId));
  }

  private static ReconstructECContainersCommand createReconstructionCmd(
      long containerId) {
    List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex> sources =
        new ArrayList<>();
    sources.add(new ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex(
        MockDatanodeDetails.randomDatanodeDetails(), 1));
    sources.add(new ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex(
        MockDatanodeDetails.randomDatanodeDetails(), 2));
    sources.add(new ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex(
        MockDatanodeDetails.randomDatanodeDetails(), 3));

    byte[] missingIndexes = new byte[1];
    missingIndexes[0] = 4;

    List<DatanodeDetails> target = singletonList(
        MockDatanodeDetails.randomDatanodeDetails());
    ReconstructECContainersCommand cmd = new ReconstructECContainersCommand(containerId, sources, target,
        ProtoUtils.unsafeByteString(missingIndexes),
        new ECReplicationConfig(3, 2));
    cmd.setTerm(CURRENT_TERM);
    return cmd;
  }

  /**
   * A fake coordinator that simulates successful reconstruction of ec containers.
   */
  private class FakeECReconstructionCoordinator extends ECReconstructionCoordinator {

    private final OzoneConfiguration conf = new OzoneConfiguration();
    private final ReplicationSupervisor supervisor;

    FakeECReconstructionCoordinator(ConfigurationSource conf,
        CertificateClient certificateClient, SecretKeySignerClient secretKeyClient,
        StateContext context, ECReconstructionMetrics metrics, String threadNamePrefix,
        ReplicationSupervisor supervisor)
            throws IOException {
      super(conf, certificateClient, secretKeyClient, context, metrics, threadNamePrefix);
      this.supervisor = supervisor;
    }

    @Override
    public void reconstructECContainerGroup(long containerID,
        ECReplicationConfig repConfig, SortedMap<Integer, DatanodeDetails> sourceNodeMap,
        SortedMap<Integer, DatanodeDetails> targetNodeMap) {
      assertEquals(1, supervisor.getTotalInFlightReplications());

      KeyValueContainerData kvcd = new KeyValueContainerData(
          containerID, layoutVersion, 100L,
          UUID.randomUUID().toString(), UUID.randomUUID().toString());
      KeyValueContainer kvc = new KeyValueContainer(kvcd, conf);
      assertDoesNotThrow(() -> {
        set.addContainer(kvc);
      });
    }
  }

  /**
   * A fake replicator that simulates successful download of containers.
   */
  private class FakeReplicator implements ContainerReplicator {

    private final OzoneConfiguration conf = new OzoneConfiguration();
    private final ReplicationSupervisor supervisor;

    FakeReplicator(ReplicationSupervisor supervisor) {
      this.supervisor = supervisor;
    }

    @Override
    public void replicate(ReplicationTask task) {
      if (set.getContainer(task.getContainerId()) != null) {
        task.setStatus(AbstractReplicationTask.Status.SKIPPED);
        return;
      }

      // assumes same-thread execution
      assertEquals(1, supervisor.getTotalInFlightReplications());

      KeyValueContainerData kvcd =
          new KeyValueContainerData(task.getContainerId(),
              layoutVersion, 100L,
              UUID.randomUUID().toString(), UUID.randomUUID().toString());
      KeyValueContainer kvc =
          new KeyValueContainer(kvcd, conf);
      assertDoesNotThrow(() -> {
        set.addContainer(kvc);
        task.setStatus(DONE);
      });
    }
  }

  /**
   * Discards all tasks.
   */
  private static class DiscardingExecutorService
      extends AbstractExecutorService {

    @Override
    public void shutdown() {
      // no-op
    }

    @Override
    public @Nonnull List<Runnable> shutdownNow() {
      return emptyList();
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) {
      return false;
    }

    @Override
    public void execute(@Nonnull Runnable command) {
      // ignore all tasks
    }
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void poolSizeCanBeIncreased() {
    datanode.setPersistedOpState(IN_SERVICE);
    ReplicationSupervisor subject = ReplicationSupervisor.newBuilder()
        .stateContext(context)
        .build();

    try {
      subject.nodeStateUpdated(ENTERING_MAINTENANCE);
    } finally {
      subject.stop();
    }
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void poolSizeCanBeDecreased() {
    datanode.setPersistedOpState(IN_MAINTENANCE);
    ReplicationSupervisor subject = ReplicationSupervisor.newBuilder()
        .stateContext(context)
        .build();

    try {
      subject.nodeStateUpdated(IN_SERVICE);
    } finally {
      subject.stop();
    }
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testMaxQueueSize() {
    List<DatanodeDetails> datanodes = new ArrayList<>();
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());

    final int maxQueueSize = 2;
    DatanodeConfiguration datanodeConfig = new DatanodeConfiguration();
    datanodeConfig.setCommandQueueLimit(maxQueueSize);

    final int replicationMaxStreams = 5;
    ReplicationServer.ReplicationConfig repConf =
        new ReplicationServer.ReplicationConfig();
    repConf.setReplicationMaxStreams(replicationMaxStreams);

    AtomicInteger threadPoolSize = new AtomicInteger();

    ReplicationSupervisor rs = ReplicationSupervisor.newBuilder()
        .executor(new DiscardingExecutorService())
        .executorThreadUpdater(threadPoolSize::set)
        .datanodeConfig(datanodeConfig)
        .replicationConfig(repConf)
        .build();

    scheduleTasks(datanodes, rs);

    // in progress task will be limited by max. queue size,
    // since all tasks are discarded by the executor, none of them complete
    assertEquals(maxQueueSize, rs.getTotalInFlightReplications());

    // queue size is doubled
    rs.nodeStateUpdated(HddsProtos.NodeOperationalState.DECOMMISSIONING);
    assertEquals(2 * maxQueueSize, rs.getMaxQueueSize());
    assertEquals(2 * replicationMaxStreams, threadPoolSize.get());

    // can schedule more tasks
    scheduleTasks(datanodes, rs);
    assertEquals(
        2 * maxQueueSize, rs.getTotalInFlightReplications());

    // queue size is restored
    rs.nodeStateUpdated(IN_SERVICE);
    assertEquals(maxQueueSize, rs.getMaxQueueSize());
    assertEquals(replicationMaxStreams, threadPoolSize.get());
  }

  //schedule 10 container replication
  private void scheduleTasks(
      List<DatanodeDetails> datanodes, ReplicationSupervisor rs) {
    for (int i = 0; i < 10; i++) {
      List<DatanodeDetails> sources =
          singletonList(datanodes.get(i % datanodes.size()));
      rs.addTask(new ReplicationTask(fromSources(i, sources), noopReplicator));
    }
  }
}
