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
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCommandInfo;
import org.apache.hadoop.ozone.container.ec.reconstruction.ECReconstructionCoordinatorTask;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.TestClock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import javax.annotation.Nonnull;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.Collections.emptyList;
import static org.apache.hadoop.ozone.container.replication.AbstractReplicationTask.Status.DONE;
import static org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand.fromSources;
import static org.junit.jupiter.api.Assertions.fail;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority.LOW;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicationCommandPriority.NORMAL;

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
  private DatanodeDetails datanode;

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
    DatanodeStateMachine stateMachine =
        Mockito.mock(DatanodeStateMachine.class);
    context = new StateContext(
        new OzoneConfiguration(),
        DatanodeStateMachine.DatanodeStates.getInitState(),
        stateMachine);
    context.setTermOfLeaderSCM(CURRENT_TERM);
    datanode = MockDatanodeDetails.randomDatanodeDetails();
    Mockito.when(stateMachine.getDatanodeDetails()).thenReturn(datanode);
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
      Assert.assertEquals(0, supervisor.getTotalInFlightReplications());
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
      Assert.assertEquals(0, supervisor.getTotalInFlightReplications());
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
      Assert.assertEquals(0, supervisor.getTotalInFlightReplications());
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
      supervisor.addTask(createECTask(4L));
      supervisor.addTask(createECTask(5L));

      //THEN
      Assert.assertEquals(0, supervisor.getReplicationRequestCount());
      Assert.assertEquals(0, supervisor.getReplicationSuccessCount());
      Assert.assertEquals(0, supervisor.getReplicationFailureCount());
      Assert.assertEquals(5, supervisor.getTotalInFlightReplications());
      Assert.assertEquals(3, supervisor.getInFlightReplications(
          ReplicationTask.class));
      Assert.assertEquals(2, supervisor.getInFlightReplications(
          ECReconstructionCoordinatorTask.class));
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
      Assert.assertEquals(3, supervisor.getTotalInFlightReplications());
      Assert.assertEquals(2, supervisor.getQueueSize());
      // Sleep 4s, wait all tasks processed
      try {
        Thread.sleep(4000);
      } catch (InterruptedException e) {
      }
      Assert.assertEquals(0, supervisor.getTotalInFlightReplications());
      Assert.assertEquals(0, supervisor.getQueueSize());
    } finally {
      supervisor.stop();
    }
  }

  @Test
  public void testDownloadAndImportReplicatorFailure() throws IOException {
    ReplicationSupervisor supervisor =
        new ReplicationSupervisor(context, newDirectExecutorService(),
            clock, 1000);

    OzoneConfiguration conf = new OzoneConfiguration();
    // Mock to fetch an exception in the importContainer method.
    SimpleContainerDownloader moc =
        Mockito.mock(SimpleContainerDownloader.class);
    Path res = Paths.get("file:/tmp/no-such-file");
    Mockito.when(
        moc.getContainerDataFromReplicas(Mockito.anyLong(), Mockito.anyList(),
            Mockito.any(Path.class), Mockito.any()))
        .thenReturn(res);

    final String testDir = GenericTestUtils.getTempPath(
        TestReplicationSupervisor.class.getSimpleName() +
            "-" + UUID.randomUUID().toString());
    MutableVolumeSet volumeSet = Mockito.mock(MutableVolumeSet.class);
    Mockito.when(volumeSet.getVolumesList())
        .thenReturn(Collections.singletonList(
            new HddsVolume.Builder(testDir).conf(conf).build()));
    ContainerImporter importer =
        new ContainerImporter(conf, set, null, volumeSet);
    ContainerReplicator replicator =
        new DownloadAndImportReplicator(conf, set, importer, moc);

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
    Assert.assertEquals(0, supervisor.getTotalInFlightReplications());
    Assert.assertEquals(0, supervisor.getQueueSize());
    Assert.assertEquals(1, supervisor.getReplicationTimeoutCount());
    Assert.assertEquals(2, set.containerCount());

  }

  @Test
  public void testDatanodeOutOfService() {
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

    Assert.assertEquals(2, supervisor.getReplicationRequestCount());
    Assert.assertEquals(1, supervisor.getReplicationSuccessCount());
    Assert.assertEquals(0, supervisor.getReplicationFailureCount());
    Assert.assertEquals(0, supervisor.getTotalInFlightReplications());
    Assert.assertEquals(0, supervisor.getQueueSize());
    Assert.assertEquals(0, supervisor.getReplicationTimeoutCount());
    Assert.assertEquals(1, set.containerCount());
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

  @Test
  public void testPriorityOrdering() throws InterruptedException {
    long deadline = clock.millis() + 1000;
    long containerId = 1;
    long term = 1;
    OzoneConfiguration conf = new OzoneConfiguration();
    ReplicationServer.ReplicationConfig repConf =
        conf.getObject(ReplicationServer.ReplicationConfig.class);
    repConf.setReplicationMaxStreams(1);
    ReplicationSupervisor supervisor =
        new ReplicationSupervisor(null, repConf, clock, 1000);

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
    Assert.assertEquals(3,
        supervisor.getInFlightReplications(OrderedTask.class));
    Assert.assertEquals(1,
        supervisor.getInFlightReplications(BlockingTask.class));

    // Unblock the queue
    completeRunning.countDown();
    // Wait for all tasks to complete
    tasksCompleteLatch.await();
    Assert.assertEquals(expectedOrder, completionOrder);
    Assert.assertEquals(0,
        supervisor.getInFlightReplications(OrderedTask.class));
    Assert.assertEquals(0,
        supervisor.getInFlightReplications(BlockingTask.class));
  }

  private static class BlockingTask extends AbstractReplicationTask {

    private CountDownLatch runningLatch;
    private CountDownLatch waitForCompleteLatch;

    BlockingTask(long containerId, long deadlineEpochMs, long term,
        CountDownLatch running, CountDownLatch waitForCompletion) {
      super(containerId, deadlineEpochMs, term);
      this.runningLatch = running;
      this.waitForCompleteLatch = waitForCompletion;
    }

    @Override
    public void runTask() {
      runningLatch.countDown();
      try {
        waitForCompleteLatch.await();
      } catch (InterruptedException e) {
        fail("Interrupted waiting for the completion latch to be released");
      }
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
    ReplicationSupervisor supervisor =
        new ReplicationSupervisor(context, executor, clock, 1000);
    replicatorRef.set(replicatorFactory.apply(supervisor));
    return supervisor;
  }

  private ReplicationTask createTask(long containerId) {
    ReplicateContainerCommand cmd = createCommand(containerId);
    return new ReplicationTask(cmd, replicatorRef.get());
  }

  private ECReconstructionCoordinatorTask createECTask(long containerId) {
    return new ECReconstructionCoordinatorTask(null,
        createReconstructionCmd(containerId));
  }

  private static ReplicateContainerCommand createCommand(long containerId) {
    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.forTest(containerId);
    cmd.setTerm(CURRENT_TERM);
    return cmd;
  }

  private static ECReconstructionCommandInfo createReconstructionCmd(
      long containerId) {
    List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex> sources
        = new ArrayList<>();
    sources.add(new ReconstructECContainersCommand
        .DatanodeDetailsAndReplicaIndex(
            MockDatanodeDetails.randomDatanodeDetails(), 1));
    sources.add(new ReconstructECContainersCommand
        .DatanodeDetailsAndReplicaIndex(
        MockDatanodeDetails.randomDatanodeDetails(), 2));
    sources.add(new ReconstructECContainersCommand
        .DatanodeDetailsAndReplicaIndex(
        MockDatanodeDetails.randomDatanodeDetails(), 3));

    byte[] missingIndexes = new byte[1];
    missingIndexes[0] = 4;

    List<DatanodeDetails> target = Collections.singletonList(
        MockDatanodeDetails.randomDatanodeDetails());
    ReconstructECContainersCommand cmd =
        new ReconstructECContainersCommand(containerId,
            sources,
            target,
            missingIndexes,
            new ECReplicationConfig(3, 2));

    return new ECReconstructionCommandInfo(cmd);
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
      Assert.assertEquals(1, supervisor.getTotalInFlightReplications());

      KeyValueContainerData kvcd =
          new KeyValueContainerData(task.getContainerId(),
              layout, 100L,
              UUID.randomUUID().toString(), UUID.randomUUID().toString());
      KeyValueContainer kvc =
          new KeyValueContainer(kvcd, conf);

      try {
        set.addContainer(kvc);
        task.setStatus(DONE);
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

  @Test
  public void testMaxQueueSize() throws InterruptedException {
    OzoneConfiguration conf = new OzoneConfiguration();
    ReplicationServer.ReplicationConfig replicationConfig
        = conf.getObject(ReplicationServer.ReplicationConfig.class);
    List<DatanodeDetails> datanodes = new ArrayList<>();
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());

    Integer[] count = new Integer[1];
    count[0] = 0;
    Lock lock = new ReentrantLock();

    ReplicationSupervisor rs = new ReplicationSupervisor(null,
        replicationConfig, Clock.system(ZoneId.systemDefault()), 2);

    ContainerReplicator replicator = task -> {
      try {
        count[0]++;
        lock.lock();
      } finally {
        lock.unlock();
      }
    };

    lock.lock();
    try {
      //schedule 100 container replication
      Random random = new Random();
      for (int i = 0; i < 100; i++) {
        List<DatanodeDetails> sources = new ArrayList<>();
        sources.add(datanodes.get(random.nextInt(datanodes.size())));
        rs.addTask(new ReplicationTask(fromSources(i, sources), replicator));
      }
      // in progress task will be 2
      Assertions.assertTrue(count[0] == 2);
    } finally {
      lock.unlock();
    }
    rs.shutdownAfterFinish();
  }
}
