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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.keyvalue.ChunkLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import org.apache.hadoop.test.GenericTestUtils;
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

  private final ContainerReplicator noopReplicator = task -> {};
  private final ContainerReplicator throwingReplicator = task -> {
    throw new RuntimeException("testing replication failure");
  };
  private final AtomicReference<ContainerReplicator> replicatorRef =
      new AtomicReference<>();
  private final ContainerReplicator mutableReplicator =
      task -> replicatorRef.get().replicate(task);

  private ContainerSet set;

  private final ChunkLayOutVersion layout;

  public TestReplicationSupervisor(ChunkLayOutVersion layout) {
    this.layout = layout;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> parameters() {
    return ChunkLayoutTestInfo.chunkLayoutParameters();
  }

  @Before
  public void setUp() throws Exception {
    set = new ContainerSet();
  }

  @After
  public void cleanup() {
    replicatorRef.set(null);
  }

  @Test
  public void normal() {
    // GIVEN
    ReplicationSupervisor supervisor = supervisorWithSuccessfulReplicator();

    try {
      //WHEN
      supervisor.addTask(new ReplicationTask(1L, emptyList()));
      supervisor.addTask(new ReplicationTask(2L, emptyList()));
      supervisor.addTask(new ReplicationTask(5L, emptyList()));

      Assert.assertEquals(3, supervisor.getReplicationRequestCount());
      Assert.assertEquals(3, supervisor.getReplicationSuccessCount());
      Assert.assertEquals(0, supervisor.getReplicationFailureCount());
      Assert.assertEquals(0, supervisor.getInFlightReplications());
      Assert.assertEquals(3, set.containerCount());
    } finally {
      supervisor.stop();
    }
  }

  @Test
  public void duplicateMessage() {
    // GIVEN
    ReplicationSupervisor supervisor = supervisorWithSuccessfulReplicator();

    try {
      //WHEN
      supervisor.addTask(new ReplicationTask(6L, emptyList()));
      supervisor.addTask(new ReplicationTask(6L, emptyList()));
      supervisor.addTask(new ReplicationTask(6L, emptyList()));
      supervisor.addTask(new ReplicationTask(6L, emptyList()));

      //THEN
      Assert.assertEquals(4, supervisor.getReplicationRequestCount());
      Assert.assertEquals(1, supervisor.getReplicationSuccessCount());
      Assert.assertEquals(0, supervisor.getReplicationFailureCount());
      Assert.assertEquals(0, supervisor.getInFlightReplications());
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
      ReplicationTask task = new ReplicationTask(1L, emptyList());
      supervisor.addTask(task);

      //THEN
      Assert.assertEquals(1, supervisor.getReplicationRequestCount());
      Assert.assertEquals(0, supervisor.getReplicationSuccessCount());
      Assert.assertEquals(1, supervisor.getReplicationFailureCount());
      Assert.assertEquals(0, supervisor.getInFlightReplications());
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
      supervisor.addTask(new ReplicationTask(1L, emptyList()));
      supervisor.addTask(new ReplicationTask(2L, emptyList()));
      supervisor.addTask(new ReplicationTask(3L, emptyList()));

      //THEN
      Assert.assertEquals(0, supervisor.getReplicationRequestCount());
      Assert.assertEquals(0, supervisor.getReplicationSuccessCount());
      Assert.assertEquals(0, supervisor.getReplicationFailureCount());
      Assert.assertEquals(3, supervisor.getInFlightReplications());
      Assert.assertEquals(0, set.containerCount());
    } finally {
      supervisor.stop();
    }
  }

  @Test
  public void testDownloadAndImportReplicatorFailure() {
    ReplicationSupervisor supervisor =
        new ReplicationSupervisor(set, mutableReplicator,
            newDirectExecutorService());

    // Mock to fetch an exception in the importContainer method.
    SimpleContainerDownloader moc =
        Mockito.mock(SimpleContainerDownloader.class);
    CompletableFuture<Path> res = new CompletableFuture<>();
    res.complete(Paths.get("file:/tmp/no-such-file"));
    Mockito.when(
        moc.getContainerDataFromReplicas(Mockito.anyLong(), Mockito.anyList()))
        .thenReturn(res);

    ContainerReplicator replicatorFactory =
        new DownloadAndImportReplicator(set, null, moc, null);

    replicatorRef.set(replicatorFactory);

    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(DownloadAndImportReplicator.LOG);

    supervisor.addTask(new ReplicationTask(1L, emptyList()));
    Assert.assertEquals(1, supervisor.getReplicationFailureCount());
    Assert.assertEquals(0, supervisor.getReplicationSuccessCount());
    Assert.assertTrue(logCapturer.getOutput()
        .contains("Container 1 replication was unsuccessful."));
  }

  private ReplicationSupervisor supervisorWithSuccessfulReplicator() {
    return supervisorWith(FakeReplicator::new, newDirectExecutorService());
  }

  private ReplicationSupervisor supervisorWith(
      Function<ReplicationSupervisor, ContainerReplicator> replicatorFactory,
      ExecutorService executor) {
    ReplicationSupervisor supervisor =
        new ReplicationSupervisor(set, mutableReplicator, executor);
    replicatorRef.set(replicatorFactory.apply(supervisor));
    return supervisor;
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
      Assert.assertNull(set.getContainer(task.getContainerId()));

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
