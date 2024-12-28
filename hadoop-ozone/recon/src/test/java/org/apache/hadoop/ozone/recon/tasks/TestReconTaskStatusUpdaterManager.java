/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.tasks;

import org.apache.hadoop.ozone.recon.metrics.ReconTaskStatusCounter;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This class contains tests to validate {@link ReconTaskStatusUpdaterManager}.
 */
public class TestReconTaskStatusUpdaterManager {
  private ReconTaskStatusDao reconTaskStatusDaoMock;
  private ReconTaskStatusCounter reconTaskStatusCounterMock;
  private ReconTaskStatusUpdaterManager manager;

  @BeforeEach
  void setup() {
    reconTaskStatusDaoMock = mock(ReconTaskStatusDao.class);
    reconTaskStatusCounterMock = mock(ReconTaskStatusCounter.class);
    when(reconTaskStatusDaoMock.findAll()).thenReturn(Collections.emptyList());
    manager = new ReconTaskStatusUpdaterManager(reconTaskStatusDaoMock, reconTaskStatusCounterMock);
  }

  @Test
  public void testGetTaskStatusUpdaterNewTask() {
    ReconTaskStatusUpdater updater = manager.getTaskStatusUpdater("task1");

    assertNotNull(updater);
    verify(reconTaskStatusDaoMock, times(1)).insert(any(ReconTaskStatus.class));
  }

  @Test
  public void testGetTaskStatusUpdaterCachedTask() {
    manager.getTaskStatusUpdater("task1");
    ReconTaskStatusUpdater updater = manager.getTaskStatusUpdater("task1");

    assertNotNull(updater);
    verify(reconTaskStatusDaoMock, times(1)).insert(any(ReconTaskStatus.class)); // Still 1 insert
  }

  @Test
  public void testInitializeWithExistingTasks() {
    ReconTaskStatus existingTask = new ReconTaskStatus("task1", 0L, 0L, 0, 0);
    when(reconTaskStatusDaoMock.findAll()).thenReturn(Collections.singletonList(existingTask));

    ReconTaskStatusUpdaterManager newManager = new ReconTaskStatusUpdaterManager(reconTaskStatusDaoMock,
        reconTaskStatusCounterMock);
    assertNotNull(newManager.getTaskStatusUpdater("task1"));

    verify(reconTaskStatusDaoMock, never()).insert(any(ReconTaskStatus.class));
  }
}
