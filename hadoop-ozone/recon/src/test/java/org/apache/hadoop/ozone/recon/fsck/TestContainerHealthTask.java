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

package org.apache.hadoop.ozone.recon.fsck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ContainerHealthTaskV2 execution flow.
 */
public class TestContainerHealthTask {

  @Test
  public void testRunTaskInvokesReconReplicationManagerProcessAll()
      throws Exception {
    ReconReplicationManager reconReplicationManager =
        mock(ReconReplicationManager.class);
    ReconStorageContainerManagerFacade reconScm =
        mock(ReconStorageContainerManagerFacade.class);
    when(reconScm.getReplicationManager()).thenReturn(reconReplicationManager);

    ContainerHealthTaskV2 task =
        new ContainerHealthTaskV2(
            createTaskConfig(),
            createTaskStatusUpdaterManagerMock(),
            reconScm);

    task.runTask();
    verify(reconReplicationManager, times(1)).processAll();
  }

  @Test
  public void testRunTaskPropagatesProcessAllFailure() throws Exception {
    ReconReplicationManager reconReplicationManager =
        mock(ReconReplicationManager.class);
    ReconStorageContainerManagerFacade reconScm =
        mock(ReconStorageContainerManagerFacade.class);
    when(reconScm.getReplicationManager()).thenReturn(reconReplicationManager);
    RuntimeException expected = new RuntimeException("processAll failed");
    org.mockito.Mockito.doThrow(expected).when(reconReplicationManager)
        .processAll();

    ContainerHealthTaskV2 task =
        new ContainerHealthTaskV2(
            createTaskConfig(),
            createTaskStatusUpdaterManagerMock(),
            reconScm);

    RuntimeException thrown =
        assertThrows(RuntimeException.class, task::runTask);
    assertEquals("processAll failed", thrown.getMessage());
  }

  private ReconTaskConfig createTaskConfig() {
    ReconTaskConfig taskConfig = new ReconTaskConfig();
    taskConfig.setMissingContainerTaskInterval(Duration.ofSeconds(2));
    return taskConfig;
  }

  private ReconTaskStatusUpdaterManager createTaskStatusUpdaterManagerMock() {
    ReconTaskStatusUpdaterManager manager =
        mock(ReconTaskStatusUpdaterManager.class);
    ReconTaskStatusUpdater updater = mock(ReconTaskStatusUpdater.class);
    when(manager.getTaskStatusUpdater("ContainerHealthTaskV2")).thenReturn(updater);
    return manager;
  }
}
