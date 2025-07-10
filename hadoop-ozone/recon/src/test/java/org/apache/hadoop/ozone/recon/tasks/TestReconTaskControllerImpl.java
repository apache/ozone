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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.ReconTaskStatus;
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
    CountDownLatch latch = new CountDownLatch(1);
    ReconOmTask reconOmTaskMock = getMockTask("MockTask");
    when(reconOmTaskMock.process(any(OMUpdateEventBatch.class), anyMap()))
        .thenAnswer(invocation -> {
          latch.countDown(); // Signal completion
          return new ReconOmTask.TaskResult.Builder().setTaskName("MockTask").setTaskSuccess(true).build();
        });
    reconTaskController.registerTask(reconOmTaskMock);
    OMUpdateEventBatch omUpdateEventBatchMock = mock(OMUpdateEventBatch.class);
    when(omUpdateEventBatchMock.getLastSequenceNumber()).thenReturn(100L);
    when(omUpdateEventBatchMock.isEmpty()).thenReturn(false);

    // Pre-initialize database record
    ReconTaskStatus initialRecord = new ReconTaskStatus("MockTask", System.currentTimeMillis(), 0L, 0, 0);
    reconTaskStatusDao.insert(initialRecord);

    long startTime = System.currentTimeMillis();
    reconTaskController.consumeOMEvents(
        omUpdateEventBatchMock,
        mock(OMMetadataManager.class));

    // Wait for async processing to complete
    assertTrue(latch.await(5, TimeUnit.SECONDS), "Task processing should complete within 5 seconds");

    verify(reconOmTaskMock, times(1))
        .process(any(), anyMap());

    ReconTaskStatus reconTaskStatus = reconTaskStatusDao.findById("MockTask");
    long taskTimeStamp = reconTaskStatus.getLastUpdatedTimestamp();
    long seqNumber = reconTaskStatus.getLastUpdatedSeqNumber();
    
    long endTime = System.currentTimeMillis();

    assertThat(taskTimeStamp).isGreaterThanOrEqualTo(startTime).isLessThanOrEqualTo(endTime);
    assertEquals(omUpdateEventBatchMock.getLastSequenceNumber(), seqNumber);
  }

  @Test
  public void testTaskRecordsFailureOnException() throws Exception {
    CountDownLatch taskExecutionLatch = new CountDownLatch(1);
    CountDownLatch dbUpdateLatch = new CountDownLatch(1);
    
    ReconOmTask reconOmTaskMock = getMockTask("MockTask");
    OMUpdateEventBatch omUpdateEventBatchMock = mock(OMUpdateEventBatch.class);

    // Throw exception when trying to run task
    when(reconOmTaskMock.process(any(OMUpdateEventBatch.class), anyMap()))
        .thenAnswer(invocation -> {
          taskExecutionLatch.countDown(); // Signal task execution
          throw new RuntimeException("Mock Failure");
        });

    // Create a custom task status updater that signals when DB is updated
    ReconTaskStatusUpdaterManager customUpdaterManager = mock(ReconTaskStatusUpdaterManager.class);
    when(customUpdaterManager.getTaskStatusUpdater(anyString()))
        .thenAnswer(i -> {
          String taskName = i.getArgument(0);
          ReconTaskStatusUpdater realUpdater = new ReconTaskStatusUpdater(reconTaskStatusDao, taskName);
          ReconTaskStatusUpdater spyUpdater = spy(realUpdater);
          
          // Signal when status is set to -1 (failure)
          doAnswer(statusInvocation -> {
            Object result = statusInvocation.callRealMethod();
            int status = statusInvocation.getArgument(0);
            if (status == -1) {
              dbUpdateLatch.countDown(); // Signal DB update for failure
            }
            return result;
          }).when(spyUpdater).setLastTaskRunStatus(anyInt());
          
          return spyUpdater;
        });

    // Create new controller with custom updater manager
    ReconTaskController customController = new ReconTaskControllerImpl(new OzoneConfiguration(), new HashSet<>(), customUpdaterManager);
    customController.start();
    customController.registerTask(reconOmTaskMock);
    
    when(omUpdateEventBatchMock.getLastSequenceNumber()).thenReturn(100L);
    when(omUpdateEventBatchMock.isEmpty()).thenReturn(false);

    // Pre-initialize database record
    ReconTaskStatus initialRecord = new ReconTaskStatus("MockTask", System.currentTimeMillis(), 0L, 0, 0);
    reconTaskStatusDao.insert(initialRecord);

    long startTime = System.currentTimeMillis();
    customController.consumeOMEvents(
        omUpdateEventBatchMock,
        mock(OMMetadataManager.class));

    // Wait for task execution and DB update
    assertTrue(taskExecutionLatch.await(5, TimeUnit.SECONDS), "Task execution should complete within 5 seconds");
    assertTrue(dbUpdateLatch.await(5, TimeUnit.SECONDS), "DB update should complete within 5 seconds");

    verify(reconOmTaskMock, times(1))
        .process(any(), anyMap());

    ReconTaskStatus reconTaskStatus = reconTaskStatusDao.findById("MockTask");
    long taskTimeStamp = reconTaskStatus.getLastUpdatedTimestamp();
    long seqNumber = reconTaskStatus.getLastUpdatedSeqNumber();
    int taskStatus = reconTaskStatus.getLastTaskRunStatus();
    
    long endTime = System.currentTimeMillis();

    assertThat(taskTimeStamp).isGreaterThanOrEqualTo(startTime).isLessThanOrEqualTo(endTime);
    // Task failed so seqNumber should not be updated, and last task status should be -1
    assertEquals(0L, seqNumber);
    assertEquals(-1, taskStatus);
    
    customController.stop();
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
    
    // Wait for async processing to complete
    Thread.sleep(2000); // Using sleep here since DummyReconDBTask doesn't allow CountDownLatch modification
    
    assertThat(reconTaskController.getRegisteredTasks()).isNotEmpty();
    assertEquals(dummyReconDBTask, reconTaskController.getRegisteredTasks()
        .get(dummyReconDBTask.getTaskName()));

    reconTaskStatusDao = getDao(ReconTaskStatusDao.class);
    ReconTaskStatus dbRecord = reconTaskStatusDao.findById(taskName);

    assertEquals(taskName, dbRecord.getTaskName());
    assertThat(dbRecord.getLastUpdatedTimestamp()).isGreaterThan(currentTime);

    assertEquals(Long.valueOf(100L), dbRecord.getLastUpdatedSeqNumber());
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

      // Wait for async processing to complete
      Thread.sleep(1000);

      assertThat(reconTaskController.getRegisteredTasks()).isNotEmpty();
      assertEquals(dummyReconDBTask, reconTaskController.getRegisteredTasks()
          .get(dummyReconDBTask.getTaskName()));
    }

    //Should be ignored now.
    Long startTime = System.currentTimeMillis();
    reconTaskController.consumeOMEvents(omUpdateEventBatchMock,
        omMetadataManagerMock);
    
    // Wait for async processing to complete
    Thread.sleep(2000);
    
    assertThat(reconTaskController.getRegisteredTasks()).isEmpty();

    reconTaskStatusDao = getDao(ReconTaskStatusDao.class);
    ReconTaskStatus dbRecord = reconTaskStatusDao.findById(taskName);

    assertEquals(taskName, dbRecord.getTaskName());
    assertThat(dbRecord.getLastUpdatedTimestamp()).isGreaterThanOrEqualTo(startTime);
    assertEquals(Long.valueOf(0L), dbRecord.getLastUpdatedSeqNumber());
  }

  @Test
  public void testReInitializeTasks() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    ReconOMMetadataManager omMetadataManagerMock = mock(
        ReconOMMetadataManager.class);
    ReconOmTask reconOmTaskMock =
        getMockTask("MockTask2");
    when(reconOmTaskMock.reprocess(omMetadataManagerMock))
        .thenAnswer(invocation -> {
          latch.countDown(); // Signal completion
          return new ReconOmTask.TaskResult.Builder().setTaskName("MockTask2").setTaskSuccess(true).build();
        });
    when(omMetadataManagerMock.getLastSequenceNumberFromDB()
    ).thenReturn(100L);

    // Pre-initialize database record
    ReconTaskStatus initialRecord = new ReconTaskStatus("MockTask2", System.currentTimeMillis(), 0L, 0, 0);
    reconTaskStatusDao.insert(initialRecord);

    long startTime = System.currentTimeMillis();
    reconTaskController.registerTask(reconOmTaskMock);
    reconTaskController.reInitializeTasks(omMetadataManagerMock, null);
    
    // Wait for async processing to complete
    assertTrue(latch.await(5, TimeUnit.SECONDS), "Task reinitialization should complete within 5 seconds");

    verify(reconOmTaskMock, times(1))
        .reprocess(omMetadataManagerMock);

    verify(omMetadataManagerMock, times(1)
    ).getLastSequenceNumberFromDB();

    ReconTaskStatus reconTaskStatus = reconTaskStatusDao.findById("MockTask2");
    long taskTimeStamp = reconTaskStatus.getLastUpdatedTimestamp();
    long seqNumber = reconTaskStatus.getLastUpdatedSeqNumber();
    
    long endTime = System.currentTimeMillis();

    assertThat(taskTimeStamp).isGreaterThanOrEqualTo(startTime).isLessThanOrEqualTo(endTime);
    assertEquals(omMetadataManagerMock.getLastSequenceNumberFromDB(), seqNumber);
  }

  /**
   * Helper method for getting a mocked Task.
   * @param taskName name of the task.
   * @return instance of reconOmTask.
   */
  private ReconOmTask getMockTask(String taskName) {
    ReconOmTask reconOmTaskMock = mock(ReconOmTask.class);
    when(reconOmTaskMock.getTaskName()).thenReturn(taskName);
    return reconOmTaskMock;
  }
}
