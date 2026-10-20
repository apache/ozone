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

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Tests for {@link PipelineSyncTask}.
 */
class TestPipelineSyncTask {

  @Test
  void keepsRunningAfterFailedSync() throws Exception {
    StorageContainerServiceProvider scmClient = mock(StorageContainerServiceProvider.class);
    AtomicReference<Thread> taskThread = new AtomicReference<>();
    when(scmClient.getPipelines()).thenAnswer(invocation -> {
      taskThread.set(Thread.currentThread());
      throw new IOException("SCM unavailable");
    }).thenReturn(emptyList());
    ReconTaskStatusUpdater updater = mock(ReconTaskStatusUpdater.class);
    ReconTaskStatusUpdaterManager manager = mock(ReconTaskStatusUpdaterManager.class);
    when(manager.getTaskStatusUpdater("PipelineSyncTask")).thenReturn(updater);
    ReconTaskConfig config = new ReconTaskConfig();
    config.setPipelineSyncTaskInterval(Duration.ofMillis(100));
    ReconPipelineManager pipelineManager = mock(ReconPipelineManager.class);
    PipelineSyncTask task = new PipelineSyncTask(pipelineManager, mock(ReconNodeManager.class),
        scmClient, config, manager);

    task.start();
    try {
      verify(pipelineManager, timeout(10_000).atLeastOnce()).initializePipelines(emptyList());
      verify(updater, timeout(10_000).atLeast(2)).recordRunCompletion();
      verify(scmClient, atLeast(2)).getPipelines();
      InOrder order = inOrder(updater);
      order.verify(updater).setLastTaskRunStatus(0);
      order.verify(updater).setLastTaskRunStatus(-1);
      order.verify(updater).recordRunCompletion();
      order.verify(updater).setLastTaskRunStatus(0);
      order.verify(updater).recordRunCompletion();
    } finally {
      task.stop();
      Thread thread = taskThread.get();
      if (thread != null) {
        thread.interrupt();
        thread.join(10_000);
        assertFalse(thread.isAlive());
      }
    }
  }
}
