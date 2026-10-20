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

package org.apache.hadoop.ozone.recon.scm;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Tests common SCM task failure handling.
 */
class TestReconScmTask {
  private ReconTaskStatusUpdater updater;
  private ReconScmTask task;
  private ReconTaskStatusUpdaterManager manager;

  @BeforeEach
  void setUp() {
    updater = mock(ReconTaskStatusUpdater.class);
    manager = mock(ReconTaskStatusUpdaterManager.class);
    when(manager.getTaskStatusUpdater(anyString())).thenReturn(updater);
    task = mock(ReconScmTask.class, org.mockito.Mockito.withSettings().useConstructor(manager)
        .defaultAnswer(org.mockito.Mockito.CALLS_REAL_METHODS));
  }

  @Test
  void testTaskCanRestartAfterThreadExits() throws Exception {
    AtomicReference<Thread> thread = new AtomicReference<>();
    CountDownLatch started = new CountDownLatch(1);
    doAnswer(invocation -> {
      thread.set(Thread.currentThread());
      started.countDown();
      return null;
    }).when(task).run();

    task.start();
    assertTrue(started.await(5, TimeUnit.SECONDS));
    thread.get().join(5000);
    assertFalse(thread.get().isAlive());
    assertFalse(task.isRunning());
    assertFalse(task.canRun());

    CountDownLatch restarted = new CountDownLatch(1);
    doAnswer(invocation -> {
      thread.set(Thread.currentThread());
      restarted.countDown();
      return null;
    }).when(task).run();
    task.start();
    assertTrue(restarted.await(5, TimeUnit.SECONDS));
    thread.get().join(5000);
    assertFalse(task.isRunning());
  }

  @Test
  void testPipelineInterruptionCompletesRunOnce() throws Exception {
    ReconTaskConfig config = new ReconTaskConfig();
    PipelineSyncTask pipelineTask = spy(new PipelineSyncTask(null, null, null, config, manager));
    doReturn(true).when(pipelineTask).canRun();
    doAnswer(invocation -> {
      throw new InterruptedException("Stopping task");
    }).when(pipelineTask).runTask();

    try {
      pipelineTask.run();
      assertTrue(Thread.currentThread().isInterrupted());
      verify(updater).recordRunCompletion();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void testPipelineSleepInterruptionDoesNotFailCompletedRun() throws Exception {
    ReconTaskConfig config = new ReconTaskConfig();
    PipelineSyncTask pipelineTask = spy(new PipelineSyncTask(null, null, null, config, manager));
    doReturn(true).when(pipelineTask).canRun();
    doAnswer(invocation -> {
      Thread.currentThread().interrupt();
      return null;
    }).when(pipelineTask).runTask();

    try {
      pipelineTask.run();
      assertTrue(Thread.currentThread().isInterrupted());
      verify(updater).recordRunCompletion();
      verify(updater, times(0)).setLastTaskRunStatus(-1);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void testFailureCompletesRunAndAllowsRetry() throws Exception {
    doThrow(new IOException("Task failed")).doNothing().when(task).runTask();

    assertDoesNotThrow(task::initializeAndRunTask);
    assertDoesNotThrow(task::initializeAndRunTask);

    InOrder order = inOrder(updater, task);
    order.verify(updater).recordRunStart();
    order.verify(updater).setLastTaskRunStatus(0);
    order.verify(task).runTask();
    order.verify(updater).setLastTaskRunStatus(-1);
    order.verify(updater).recordRunCompletion();
    order.verify(updater).recordRunStart();
    order.verify(updater).setLastTaskRunStatus(0);
    order.verify(task).runTask();
    order.verify(updater).recordRunCompletion();
  }

  @Test
  void testPipelineSyncRetriesAfterFailure() throws Exception {
    ReconTaskConfig config = new ReconTaskConfig();
    config.setPipelineSyncTaskInterval(Duration.ofMillis(10));
    PipelineSyncTask pipelineTask = spy(new PipelineSyncTask(null, null, null, config, manager));
    doReturn(true, true, false).when(pipelineTask).canRun();
    doThrow(new IOException("SCM unavailable")).doNothing().when(pipelineTask).runTask();

    pipelineTask.run();

    verify(pipelineTask, times(2)).runTask();
    verify(updater, times(2)).recordRunCompletion();
  }

  @Test
  void testErrorPropagatesAndCompletesFailedRunOnce() throws Exception {
    AssertionError error = new AssertionError("Task failed with an error");
    doThrow(error).when(task).runTask();

    assertSame(error, assertThrows(AssertionError.class, task::initializeAndRunTask));

    InOrder order = inOrder(updater, task);
    order.verify(updater).recordRunStart();
    order.verify(updater).setLastTaskRunStatus(0);
    order.verify(task).runTask();
    order.verify(updater).setLastTaskRunStatus(-1);
    order.verify(updater).recordRunCompletion();
    verify(updater).recordRunCompletion();
    verify(task).runTask();
  }

  @Test
  void testInterruptionPropagatesAndCompletesRun() throws Exception {
    InterruptedException interruption = new InterruptedException("Stopping task");
    doThrow(interruption).when(task).runTask();

    assertSame(interruption, assertThrows(InterruptedException.class, task::initializeAndRunTask));

    InOrder order = inOrder(updater);
    order.verify(updater).recordRunStart();
    order.verify(updater).setLastTaskRunStatus(0);
    order.verify(updater).setLastTaskRunStatus(-1);
    order.verify(updater).recordRunCompletion();
  }
}
