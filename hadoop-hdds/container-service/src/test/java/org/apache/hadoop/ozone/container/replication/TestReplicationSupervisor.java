/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.replication;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.TestClock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import javax.annotation.Nonnull;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.Collections.emptyList;

/**
 * Test the replication supervisor.
 */
@RunWith(Parameterized.class)
public class TestReplicationSupervisor {

  private static final long CURRENT_TERM = 1;

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
  private final AtomicReference<ContainerReplicator> pushReplicatorRef =
      new AtomicReference<>();
  private final ContainerReplicator pullReplicator =
      task -> replicatorRef.get().replicate(task);
  private final ContainerReplicator pushReplicator =
      task -> pushReplicatorRef.get().replicate(task);

  private ContainerSet set;

  private final ContainerLayoutVersion layout;

  private StateContext context;
  private TestClock clock;

  public TestReplicationSupervisor(ContainerLayoutVersion layout) {
    this.layout = layout;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ContainerLayoutTestInfo.containerLayoutParameters();
  }

  @Before
  public void setUp() throws Exception {
    clock = new TestClock(Instant.now(), ZoneId.systemDefault());
    set = new ContainerSet(1000);
    context = new StateContext(
        new OzoneConfiguration(),
        DatanodeStateMachine.DatanodeStates.getInitState(),
        Mockito.mock(DatanodeStateMachine.class));
    context.setTermOfLeaderSCM(CURRENT_TERM);
  }

  @After
  public void cleanup() {
    replicatorRef.set(null);
  }

  @Test
  public void normal() {
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

      Assert.assertEquals(3, supervisor.getReplicationRequestCount());
      Assert.assertEquals(3, supervisor.getReplicationSuccessCount());
      Assert.assertEquals(0, supervisor.getReplicationFailureCount());
      Assert.assertEquals(0, supervisor.getInFlightReplications());
      Assert.assertEquals(0, supervisor.getQueueSize());
      Assert.assertEquals(3, set.containerCount());

      MetricsCollectorImpl metricsCollector = new MetricsCollectorImpl();
      metrics.getMetrics(metricsCollector, true);
      Assert.assertEquals(1, metricsCollector.getRecords().size());
    } finally {
      metrics.unRegister();
      supervisor.stop();
    }
  }

  @Test
  public void duplicateMessage() {
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
      Assert.assertEquals(4, supervisor.getReplicationRequestCount());
      Assert.assertEquals(1, supervisor.getReplicationSuccessCount());
      Assert.assertEquals(0, supervisor.getReplicationFailureCount());
      Assert.assertEquals(3, supervisor.getReplicationSkippedCount());
      Assert.assertEquals(0, supervisor.getInFlightReplications());
      Assert.assertEquals(0, supervisor.getQueueSize());
      Assert.assertEquals(1, set.containerCount());
    } finally {
      supervisor.stop();
    }
  }

  @Test
  public void failureHandling() {
    // GIVEN
    ReplicationSupervisor supervisor = supervisorWith(
        __ -> throwingReplicator, newDirectExecutorService());

    try {
      //WHEN
      ReplicationTask task = createTask(1L);
      supervisor.addTask(task);

      //THEN
      Assert.assertEquals(1, supervisor.getReplicationRequestCount());
      Assert.assertEquals(0, supervisor.getReplicationSuccessCount());
      Assert.assertEquals(1, supervisor.getReplicationFailureCount());
      Assert.assertEquals(0, supervisor.getInFlightReplications());
      Assert.assertEquals(0, supervisor.getQueueSize());
      Assert.assertEquals(0, set.containerCount());
      Assert.assertEquals(ReplicationTask.Status.FAILED, task.getStatus());
    } finally {
      supervisor.stop();
    }
  }

  @Test
  public void stalledDownload() {
    // GIVEN
    ReplicationSupervisor supervisor = supervisorWith(__ -> noopReplicator,
        new DiscardingExecutorService());

    try {
      //WHEN
      supervisor.addTask(createTask(1L));
      supervisor.addTask(createTask(2L));
      supervisor.addTask(createTask(3L));

      //THEN
      Assert.assertEquals(0, supervisor.getReplicationRequestCount());
      Assert.assertEquals(0, supervisor.getReplicationSuccessCount());
      Assert.assertEquals(0, supervisor.getReplicationFailureCount());
      Assert.assertEquals(3, supervisor.getInFlightReplications());
      Assert.assertEquals(0, supervisor.getQueueSize());
      Assert.assertEquals(0, set.containerCount());
    } finally {
      supervisor.stop();
    }
  }

  @Test
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
      Assert.assertEquals(3, supervisor.getInFlightReplications());
      Assert.assertEquals(2, supervisor.getQueueSize());
      // Sleep 4s, wait all tasks processed
      try {
        Thread.sleep(4000);
      } catch (InterruptedException e) {
      }
      Assert.assertEquals(0, supervisor.getInFlightReplications());
      Assert.assertEquals(0, supervisor.getQueueSize());
    } finally {
      supervisor.stop();
    }
  }

  @Test
  public void testDownloadAndImportReplicatorFailure() throws IOException {
    ReplicationSupervisor supervisor =
        new ReplicationSupervisor(context, newDirectExecutorService(), clock);

    OzoneConfiguration conf = new OzoneConfiguration();
    // Mock to fetch an exception in the importContainer method.
    SimpleContainerDownloader moc =
        Mockito.mock(SimpleContainerDownloader.class);
    Path res = Paths.get("file:/tmp/no-such-file");
    Mockito.when(
        moc.getContainerDataFromReplicas(Mockito.anyLong(), Mockito.anyList(),
            Mockito.any(Path.class)))
        .thenReturn(res);

    final String testDir = GenericTestUtils.getTempPath(
        TestReplicationSupervisor.class.getSimpleName() +
            "-" + UUID.randomUUID().toString());
    MutableVolumeSet volumeSet = Mockito.mock(MutableVolumeSet.class);
    Mockito.when(volumeSet.getVolumesList())
        .thenReturn(Collections.singletonList(
            new HddsVolume.Builder(testDir).conf(conf).build()));
    ContainerImporter importer =
        new ContainerImporter(conf, set, null, null, volumeSet);
    ContainerReplicator replicator =
        new DownloadAndImportReplicator(set, importer, moc);

    replicatorRef.set(replicator);

    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(DownloadAndImportReplicator.LOG);

    supervisor.addTask(createTask(1L));
    Assert.assertEquals(1, supervisor.getReplicationFailureCount());
    Assert.assertEquals(0, supervisor.getReplicationSuccessCount());
    Assert.assertTrue(logCapturer.getOutput()
        .contains("Container 1 replication was unsuccessful."));
  }

  @Test
  public void testTaskBeyondDeadline() {
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

    Assert.assertEquals(3, supervisor.getReplicationRequestCount());
    Assert.assertEquals(2, supervisor.getReplicationSuccessCount());
    Assert.assertEquals(0, supervisor.getReplicationFailureCount());
    Assert.assertEquals(0, supervisor.getInFlightReplications());
    Assert.assertEquals(0, supervisor.getQueueSize());
    Assert.assertEquals(1, supervisor.getReplicationTimeoutCount());
    Assert.assertEquals(2, set.containerCount());

  }

  @Test
  public void taskWithObsoleteTermIsDropped() {
    final long newTerm = 2;
    ReplicationSupervisor supervisor =
        supervisorWithReplicator(FakeReplicator::new);

    context.setTermOfLeaderSCM(newTerm);
    supervisor.addTask(createTask(1L));

    Assert.assertEquals(1, supervisor.getReplicationRequestCount());
    Assert.assertEquals(0, supervisor.getReplicationSuccessCount());
  }

  private ReplicationSupervisor supervisorWithReplicator(
      Function<ReplicationSupervisor, ContainerReplicator> replicatorFactory) {
    return supervisorWith(replicatorFactory, newDirectExecutorService());
  }

  private ReplicationSupervisor supervisorWith(
      Function<ReplicationSupervisor, ContainerReplicator> replicatorFactory,
      ExecutorService executor) {
    ReplicationSupervisor supervisor =
        new ReplicationSupervisor(context, executor, clock);
    replicatorRef.set(replicatorFactory.apply(supervisor));
    return supervisor;
  }

  private ReplicationTask createTask(long containerId) {
    ReplicateContainerCommand cmd = createCommand(containerId);
    return new ReplicationTask(cmd, replicatorRef.get());
  }

  private static ReplicateContainerCommand createCommand(long containerId) {
    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.forTest(containerId);
    cmd.setTerm(CURRENT_TERM);
    return cmd;
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
      Assert.assertEquals(1, supervisor.getInFlightReplications());

      KeyValueContainerData kvcd =
          new KeyValueContainerData(task.getContainerId(),
              layout, 100L,
              UUID.randomUUID().toString(), UUID.randomUUID().toString());
      KeyValueContainer kvc =
          new KeyValueContainer(kvcd, conf);

      try {
        set.addContainer(kvc);
        task.setStatus(ReplicationTask.Status.DONE);
      } catch (Exception e) {
        Assert.fail("Unexpected error: " + e.getMessage());
      }
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
}
