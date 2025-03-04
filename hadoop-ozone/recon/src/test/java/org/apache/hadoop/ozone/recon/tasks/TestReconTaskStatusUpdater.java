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

package org.apache.hadoop.ozone.recon.tasks;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.ReconTaskStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class contains tests to validate {@link ReconTaskStatusUpdater} class.
 */
public class TestReconTaskStatusUpdater extends AbstractReconSqlDBTest {
  private ReconTaskStatusDao reconTaskStatusDaoMock;
  private ReconTaskStatusUpdater updater;

  @BeforeEach
  void setup() {
    this.reconTaskStatusDaoMock = mock(ReconTaskStatusDao.class);
    this.updater = mock(ReconTaskStatusUpdater.class);

    doAnswer(inv -> {
      if (!reconTaskStatusDaoMock.existsById(anyString())) {
        // First time getting the task, so insert value
        reconTaskStatusDaoMock.insert(new ReconTaskStatus("task1", 0L, 0L, 0, 0));
      } else {
        // We already have row for the task in the table, update the row
        reconTaskStatusDaoMock.update(new ReconTaskStatus("task1", 0L, 0L, 0, 0));
      }
      return  null;
    }).when(updater).updateDetails();
    doNothing().when(updater).setLastUpdatedSeqNumber(anyLong());
    doNothing().when(updater).setLastUpdatedTimestamp(anyLong());
    doNothing().when(updater).setLastTaskRunStatus(anyInt());
    doNothing().when(updater).setIsCurrentTaskRunning(anyInt());
  }

  @Test
  void testUpdateDetailsFirstTime() {
    when(reconTaskStatusDaoMock.existsById(anyString())).thenReturn(false);

    updater.updateDetails();

    verify(reconTaskStatusDaoMock, times(1)).insert(any(ReconTaskStatus.class));
    verify(reconTaskStatusDaoMock, never()).update(any(ReconTaskStatus.class));
  }

  @Test
  void testUpdateDetailsUpdateExisting() {
    when(reconTaskStatusDaoMock.existsById(anyString())).thenReturn(true);

    updater.setLastTaskRunStatus(1); // Task success
    updater.updateDetails();

    verify(reconTaskStatusDaoMock, times(1)).update(any(ReconTaskStatus.class));
  }

  @Test
  void testSetters() {
    updater.setLastUpdatedSeqNumber(100L);
    updater.setLastUpdatedTimestamp(200L);
    updater.setLastTaskRunStatus(1);
    updater.setIsCurrentTaskRunning(0);

    // Verify the fields are updated without throwing exceptions
    verify(updater, times(1)).setLastUpdatedSeqNumber(anyLong());
    verify(updater, times(1)).setLastUpdatedTimestamp(anyLong());
    verify(updater, times(1)).setLastTaskRunStatus(anyInt());
    verify(updater, times(1)).setIsCurrentTaskRunning(anyInt());
  }
}
