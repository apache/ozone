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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
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
  public void setUp() throws IOException {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    reconTaskStatusDao = getDao(ReconTaskStatusDao.class);
    ReconTaskStatusUpdaterManager reconTaskStatusUpdaterManagerMock = mock(ReconTaskStatusUpdaterManager.class);
    when(reconTaskStatusUpdaterManagerMock.getTaskStatusUpdater(anyString()))
        .thenAnswer(i -> {
          String taskName = i.getArgument(0);
          return new ReconTaskStatusUpdater(reconTaskStatusDao, taskName);
        });
    ReconDBProvider reconDbProvider = mock(ReconDBProvider.class);
    when(reconDbProvider.getDbStore()).thenReturn(mock(DBStore.class));
    when(reconDbProvider.getStagedReconDBProvider()).thenReturn(reconDbProvider);
    ReconContainerMetadataManager reconContainerMgr = mock(ReconContainerMetadataManager.class);
    ReconNamespaceSummaryManager nsSummaryManager = mock(ReconNamespaceSummaryManager.class);
    reconTaskController = new ReconTaskControllerImpl(ozoneConfiguration, new HashSet<>(),
        reconTaskStatusUpdaterManagerMock, reconDbProvider, reconContainerMgr, nsSummaryManager);
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
    // Use CountDownLatch to wait for async processing
    CountDownLatch taskCompletionLatch = new CountDownLatch(1);
    
    ReconOmTask reconOmTaskMock = getMockTask("MockTask");
    when(reconOmTaskMock.process(any(OMUpdateEventBatch.class), anyMap()))
        .thenAnswer(invocation -> {
          taskCompletionLatch.countDown(); // Signal task completion
          return new ReconOmTask.TaskResult.Builder().setTaskName("MockTask").setTaskSuccess(true).build();
        });
    reconTaskController.registerTask(reconOmTaskMock);
    
    OMUpdateEventBatch omUpdateEventBatchMock = mock(OMUpdateEventBatch.class);
    when(omUpdateEventBatchMock.getLastSequenceNumber()).thenReturn(100L);
    when(omUpdateEventBatchMock.isEmpty()).thenReturn(false);
    when(omUpdateEventBatchMock.getEvents()).thenReturn(new ArrayList<>());
    when(omUpdateEventBatchMock.getEventType()).thenReturn(ReconEvent.EventType.OM_UPDATE_BATCH);
    when(omUpdateEventBatchMock.getEventCount()).thenReturn(1);

    long startTime = System.currentTimeMillis();
    reconTaskController.consumeOMEvents(
        omUpdateEventBatchMock,
        mock(OMMetadataManager.class));

    // Wait for async processing to complete using latch
    boolean completed = taskCompletionLatch.await(10, TimeUnit.SECONDS);
    assertThat(completed).isTrue();
    
    verify(reconOmTaskMock, times(1))
        .process(any(), anyMap());
    long endTime = System.currentTimeMillis();

    ReconTaskStatus reconTaskStatus = reconTaskStatusDao.findById("MockTask");
    long taskTimeStamp = reconTaskStatus.getLastUpdatedTimestamp();
    long seqNumber = reconTaskStatus.getLastUpdatedSeqNumber();

    assertThat(taskTimeStamp).isGreaterThanOrEqualTo(startTime).isLessThanOrEqualTo(endTime);
    assertEquals(omUpdateEventBatchMock.getLastSequenceNumber(), seqNumber);
  }

  @Test
  public void testTaskRecordsFailureOnException() throws Exception {
    // Use CountDownLatch to wait for async processing
    CountDownLatch taskCompletionLatch = new CountDownLatch(1);
    
    ReconOmTask reconOmTaskMock = getMockTask("MockTask");
    OMUpdateEventBatch omUpdateEventBatchMock = mock(OMUpdateEventBatch.class);

    // Throw exception when trying to run task, but still signal completion
    when(reconOmTaskMock.process(any(OMUpdateEventBatch.class), anyMap()))
        .thenAnswer(invocation -> {
          taskCompletionLatch.countDown(); // Signal task completion
          throw new RuntimeException("Mock Failure");
        });
    reconTaskController.registerTask(reconOmTaskMock);
    when(omUpdateEventBatchMock.getLastSequenceNumber()).thenReturn(100L);
    when(omUpdateEventBatchMock.isEmpty()).thenReturn(false);
    when(omUpdateEventBatchMock.getEvents()).thenReturn(new ArrayList<>());
    when(omUpdateEventBatchMock.getEventType()).thenReturn(ReconEvent.EventType.OM_UPDATE_BATCH);
    when(omUpdateEventBatchMock.getEventCount()).thenReturn(1);

    long startTime = System.currentTimeMillis();
    reconTaskController.consumeOMEvents(
        omUpdateEventBatchMock,
        mock(OMMetadataManager.class));

    // Wait for async processing to complete using latch
    boolean completed = taskCompletionLatch.await(10, TimeUnit.SECONDS);
    assertThat(completed).isTrue();
    
    // Wait a bit more for task status to be recorded after the exception
    Thread.sleep(500);
    
    verify(reconOmTaskMock, times(1))
        .process(any(), anyMap());
    long endTime = System.currentTimeMillis();

    ReconTaskStatus reconTaskStatus = reconTaskStatusDao.findById("MockTask");
    long taskTimeStamp = reconTaskStatus.getLastUpdatedTimestamp();
    long seqNumber = reconTaskStatus.getLastUpdatedSeqNumber();
    int taskStatus = reconTaskStatus.getLastTaskRunStatus();

    assertThat(taskTimeStamp).isGreaterThanOrEqualTo(startTime).isLessThanOrEqualTo(endTime);
    // Task failed so seqNumber should not be updated, and last task status should be -1
    assertEquals(0, seqNumber);
    assertEquals(-1, taskStatus);
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

    when(omUpdateEventBatchMock.getEvents()).thenReturn(new ArrayList<>());
    when(omUpdateEventBatchMock.getEventType()).thenReturn(ReconEvent.EventType.OM_UPDATE_BATCH);
    when(omUpdateEventBatchMock.getEventCount()).thenReturn(1);
    
    reconTaskController.consumeOMEvents(omUpdateEventBatchMock,
        mock(OMMetadataManager.class));
    
    // Wait for async processing to complete
    Thread.sleep(3000); // Increase timeout for retry logic
    
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
  @org.junit.jupiter.api.Disabled("Task removal logic not implemented in async processing")
  public void testBadBehavedTaskIsIgnored() throws Exception {
    String taskName = "Dummy_" + System.currentTimeMillis();
    DummyReconDBTask dummyReconDBTask =
        new DummyReconDBTask(taskName, DummyReconDBTask.TaskType.ALWAYS_FAIL);
    reconTaskController.registerTask(dummyReconDBTask);

    OMUpdateEventBatch omUpdateEventBatchMock = mock(OMUpdateEventBatch.class);
    when(omUpdateEventBatchMock.isEmpty()).thenReturn(false);
    when(omUpdateEventBatchMock.getLastSequenceNumber()).thenReturn(100L);

    when(omUpdateEventBatchMock.getEvents()).thenReturn(new ArrayList<>());
    
    OMMetadataManager omMetadataManagerMock = mock(OMMetadataManager.class);
    for (int i = 0; i < 2; i++) {
      reconTaskController.consumeOMEvents(omUpdateEventBatchMock,
          omMetadataManagerMock);
      
      // Wait for async processing to complete
      Thread.sleep(2000);

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

    ReconOMMetadataManager omMetadataManagerMock = mock(
        ReconOMMetadataManager.class);
    ReconOmTask reconOmTaskMock =
        getMockTask("MockTask2");
    when(reconOmTaskMock.getStagedTask(any(), any())).thenReturn(reconOmTaskMock);
    when(reconOmTaskMock.reprocess(omMetadataManagerMock))
        .thenReturn(new ReconOmTask.TaskResult.Builder().setTaskName("MockTask2").setTaskSuccess(true).build());
    when(omMetadataManagerMock.getLastSequenceNumberFromDB()
    ).thenReturn(100L);

    long startTime = System.currentTimeMillis();
    reconTaskController.registerTask(reconOmTaskMock);
    reconTaskController.reInitializeTasks(omMetadataManagerMock, null);
    long endTime = System.currentTimeMillis();

    verify(reconOmTaskMock, times(1))
        .reprocess(omMetadataManagerMock);

    verify(omMetadataManagerMock, times(1)
    ).getLastSequenceNumberFromDB();

    ReconTaskStatus reconTaskStatus = reconTaskStatusDao.findById("MockTask2");
    long taskTimeStamp = reconTaskStatus.getLastUpdatedTimestamp();
    long seqNumber = reconTaskStatus.getLastUpdatedSeqNumber();

    ReconTaskStatus reprocessStaging = reconTaskStatusDao.findById("REPROCESS_STAGING");
    assertEquals(omMetadataManagerMock.getLastSequenceNumberFromDB(), reprocessStaging.getLastUpdatedSeqNumber());
    assertEquals(0, reprocessStaging.getLastTaskRunStatus());

    assertThat(taskTimeStamp).isGreaterThanOrEqualTo(startTime).isLessThanOrEqualTo(endTime);
    assertEquals(seqNumber,
        omMetadataManagerMock.getLastSequenceNumberFromDB());
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
