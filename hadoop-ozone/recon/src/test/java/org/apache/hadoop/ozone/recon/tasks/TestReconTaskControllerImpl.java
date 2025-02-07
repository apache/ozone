/**
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import java.util.HashSet;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Class used to test ReconTaskControllerImpl.
 */
public class TestReconTaskControllerImpl extends AbstractReconSqlDBTest {

  private ReconTaskController reconTaskController;
  private ReconTaskStatusDao reconTaskStatusDao;

  public TestReconTaskControllerImpl() {
    super();
  }

  @BeforeEach
  public void setUp() {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    reconTaskStatusDao = getDao(ReconTaskStatusDao.class);
    ReconTaskStatusUpdaterManager reconTaskStatusUpdaterManagerMock = mock(ReconTaskStatusUpdaterManager.class);
    when(reconTaskStatusUpdaterManagerMock.getTaskStatusUpdater(anyString()))
        .thenAnswer(i -> {
          String taskName = i.getArgument(0);
          return new ReconTaskStatusUpdater(reconTaskStatusDao, taskName);
        });
    reconTaskController = new ReconTaskControllerImpl(ozoneConfiguration, new HashSet<>(),
        reconTaskStatusUpdaterManagerMock);
    reconTaskController.start();
  }

  @Test
  public void testRegisterTask() {
    String taskName = "Dummy_" + System.currentTimeMillis();
    DummyReconDBTask dummyReconDBTask =
        new DummyReconDBTask(taskName, DummyReconDBTask.TaskType.ALWAYS_PASS);
    reconTaskController.registerTask(dummyReconDBTask);
    assertEquals(1, reconTaskController.getRegisteredTasks().size());
    assertSame(reconTaskController.getRegisteredTasks()
        .get(dummyReconDBTask.getTaskName()), dummyReconDBTask);
  }

  @Test
  public void testConsumeOMEvents() throws Exception {
    String taskName = "Dummy_" + System.currentTimeMillis();
    DummyReconDBTask dummyReconTask = new DummyReconDBTask(taskName, DummyReconDBTask.TaskType.ALWAYS_PASS);
    reconTaskController.registerTask(dummyReconTask);
    OMUpdateEventBatch omUpdateEventBatchMock = mock(OMUpdateEventBatch.class);
    when(omUpdateEventBatchMock.getLastSequenceNumber()).thenReturn(100L);
    when(omUpdateEventBatchMock.isEmpty()).thenReturn(false);

    long startTime = System.currentTimeMillis();
    reconTaskController.consumeOMEvents(
        omUpdateEventBatchMock,
        mock(OMMetadataManager.class));

    verify(dummyReconTask, times(1))
        .process(any());
    long endTime = System.currentTimeMillis();

    ReconTaskStatus reconTaskStatus = reconTaskStatusDao.findById(taskName);
    long taskTimeStamp = reconTaskStatus.getLastUpdatedTimestamp();
    long seqNumber = reconTaskStatus.getLastUpdatedSeqNumber();

    assertThat(taskTimeStamp).isGreaterThanOrEqualTo(startTime).isLessThanOrEqualTo(endTime);
    assertEquals(seqNumber, omUpdateEventBatchMock.getLastSequenceNumber());
    assertEquals(1, dummyReconTask.getMetricsInstance().getTaskProcessCount());
  }

  @Test
  public void testTaskRecordsFailureOnException() throws Exception {
    String taskName = "Dummy_" + System.currentTimeMillis();
    DummyReconDBTask dummyReconTask = new DummyReconDBTask(taskName, DummyReconDBTask.TaskType.FAIL_ONCE);
    OMUpdateEventBatch omUpdateEventBatchMock = mock(OMUpdateEventBatch.class);

    reconTaskController.registerTask(dummyReconTask);
    when(omUpdateEventBatchMock.getLastSequenceNumber()).thenReturn(100L);
    when(omUpdateEventBatchMock.isEmpty()).thenReturn(false);

    long startTime = System.currentTimeMillis();
    reconTaskController.consumeOMEvents(
        omUpdateEventBatchMock,
        mock(OMMetadataManager.class));

    verify(dummyReconTask, times(1))
        .process(any());
    long endTime = System.currentTimeMillis();

    ReconTaskStatus reconTaskStatus = reconTaskStatusDao.findById(taskName);
    long taskTimeStamp = reconTaskStatus.getLastUpdatedTimestamp();
    long seqNumber = reconTaskStatus.getLastUpdatedSeqNumber();
    int taskStatus = reconTaskStatus.getLastTaskRunStatus();

    assertThat(taskTimeStamp).isGreaterThanOrEqualTo(startTime).isLessThanOrEqualTo(endTime);
    // Task failed so seqNumber should not be updated, and last task status should be -1
    assertEquals(seqNumber, 0);
    assertEquals(taskStatus, -1);
    assertEquals(1, dummyReconTask.getMetricsInstance().getTaskProcessCount());
    assertEquals(1, dummyReconTask.getMetricsInstance().getTaskProcessFailureCount());
  }

  @Test
  public void testFailedTaskRetryLogic() throws Exception {
    String taskName = "Dummy_" + System.currentTimeMillis();

    DummyReconDBTask dummyReconDBTask =
        new DummyReconDBTask(taskName, DummyReconDBTask.TaskType.FAIL_ONCE);
    reconTaskController.registerTask(dummyReconDBTask);

    long currentTime = System.currentTimeMillis();
    OMUpdateEventBatch omUpdateEventBatchMock = mock(OMUpdateEventBatch.class);
    when(omUpdateEventBatchMock.isEmpty()).thenReturn(false);
    when(omUpdateEventBatchMock.getLastSequenceNumber()).thenReturn(100L);

    reconTaskController.consumeOMEvents(omUpdateEventBatchMock,
        mock(OMMetadataManager.class));
    assertThat(reconTaskController.getRegisteredTasks()).isNotEmpty();
    assertEquals(dummyReconDBTask, reconTaskController.getRegisteredTasks()
        .get(dummyReconDBTask.getTaskName()));

    reconTaskStatusDao = getDao(ReconTaskStatusDao.class);
    ReconTaskStatus dbRecord = reconTaskStatusDao.findById(taskName);

    assertEquals(taskName, dbRecord.getTaskName());
    assertThat(dbRecord.getLastUpdatedTimestamp()).isGreaterThan(currentTime);

    assertEquals(Long.valueOf(100L), dbRecord.getLastUpdatedSeqNumber());
    assertEquals(1, dummyReconDBTask.getMetricsInstance().getTaskProcessCount());
    assertEquals(1, dummyReconDBTask.getMetricsInstance().getTaskProcessFailureCount());
  }

  @Test
  public void testBadBehavedTaskIsIgnored() throws Exception {
    String taskName = "Dummy_" + System.currentTimeMillis();
    DummyReconDBTask dummyReconDBTask =
        new DummyReconDBTask(taskName, DummyReconDBTask.TaskType.ALWAYS_FAIL);
    reconTaskController.registerTask(dummyReconDBTask);

    OMUpdateEventBatch omUpdateEventBatchMock = mock(OMUpdateEventBatch.class);
    when(omUpdateEventBatchMock.isEmpty()).thenReturn(false);
    when(omUpdateEventBatchMock.getLastSequenceNumber()).thenReturn(100L);

    OMMetadataManager omMetadataManagerMock = mock(OMMetadataManager.class);
    for (int i = 0; i < 2; i++) {
      reconTaskController.consumeOMEvents(omUpdateEventBatchMock,
          omMetadataManagerMock);

      assertThat(reconTaskController.getRegisteredTasks()).isNotEmpty();
      assertEquals(dummyReconDBTask, reconTaskController.getRegisteredTasks()
          .get(dummyReconDBTask.getTaskName()));
      assertEquals(3, dummyReconDBTask.getMetricsInstance().getTaskProcessCount());
      assertEquals(3, dummyReconDBTask.getMetricsInstance().getTaskProcessFailureCount());
    }

    //Should be ignored now.
    Long startTime = System.currentTimeMillis();
    reconTaskController.consumeOMEvents(omUpdateEventBatchMock,
        omMetadataManagerMock);
    assertThat(reconTaskController.getRegisteredTasks()).isEmpty();

    reconTaskStatusDao = getDao(ReconTaskStatusDao.class);
    ReconTaskStatus dbRecord = reconTaskStatusDao.findById(taskName);

    assertEquals(taskName, dbRecord.getTaskName());
    assertThat(dbRecord.getLastUpdatedTimestamp()).isGreaterThanOrEqualTo(startTime);
    assertEquals(Long.valueOf(0L), dbRecord.getLastUpdatedSeqNumber());
  }


  @Test
  public void testReInitializeTasks() throws Exception {
    String taskName = "Dummy_" + System.currentTimeMillis();
    DummyReconDBTask dummyReconDBTask = new DummyReconDBTask(taskName, DummyReconDBTask.TaskType.ALWAYS_PASS);
    ReconOMMetadataManager omMetadataManagerMock = mock(
        ReconOMMetadataManager.class);
    when(omMetadataManagerMock.getLastSequenceNumberFromDB()
    ).thenReturn(100L);

    long startTime = System.currentTimeMillis();
    reconTaskController.registerTask(dummyReconDBTask);
    reconTaskController.reInitializeTasks(omMetadataManagerMock);
    long endTime = System.currentTimeMillis();

    verify(dummyReconDBTask, times(1))
        .reprocess(omMetadataManagerMock);

    verify(omMetadataManagerMock, times(1)
    ).getLastSequenceNumberFromDB();

    ReconTaskStatus reconTaskStatus = reconTaskStatusDao.findById(taskName);
    long taskTimeStamp = reconTaskStatus.getLastUpdatedTimestamp();
    long seqNumber = reconTaskStatus.getLastUpdatedSeqNumber();

    assertThat(taskTimeStamp).isGreaterThanOrEqualTo(startTime).isLessThanOrEqualTo(endTime);
    assertEquals(seqNumber,
        omMetadataManagerMock.getLastSequenceNumberFromDB());
    assertEquals(1, dummyReconDBTask.getMetricsInstance().getTaskReprocessCount());
  }
}
