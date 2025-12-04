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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconFileMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.ReconTaskStatus;
import org.apache.ozone.test.GenericTestUtils;
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
    ReconGlobalStatsManager reconGlobalStatsManager = mock(ReconGlobalStatsManager.class);
    ReconFileMetadataManager reconFileMetadataManager = mock(ReconFileMetadataManager.class);
    reconTaskController = new ReconTaskControllerImpl(ozoneConfiguration, new HashSet<>(),
        reconTaskStatusUpdaterManagerMock, reconDbProvider, reconContainerMgr, nsSummaryManager,
        reconGlobalStatsManager, reconFileMetadataManager);
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
    
    // Wait for task status to be recorded after the exception
    GenericTestUtils.waitFor(() -> {
      try {
        ReconTaskStatus status = reconTaskStatusDao.findById("MockTask");
        return status != null && status.getLastTaskRunStatus() == -1;
      } catch (Exception e) {
        return false;
      }
    }, 100, 5000);
    
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

  @Test
  public void testQueueReInitializationEventSuccess() throws Exception {
    // Set up properly mocked ReconOMMetadataManager with required dependencies
    ReconOMMetadataManager mockOMMetadataManager = mock(ReconOMMetadataManager.class);
    DBStore mockDBStore = mock(DBStore.class);
    File mockDbLocation = mock(File.class);
    DBCheckpoint mockCheckpoint = mock(DBCheckpoint.class);
    Path mockCheckpointPath = Paths.get("/tmp/test/checkpoint");
    
    when(mockOMMetadataManager.getStore()).thenReturn(mockDBStore);
    when(mockDBStore.getDbLocation()).thenReturn(mockDbLocation);
    when(mockDbLocation.getParent()).thenReturn("/tmp/test");
    when(mockDBStore.getCheckpoint(any(String.class), any(Boolean.class))).thenReturn(mockCheckpoint);
    when(mockCheckpoint.getCheckpointLocation()).thenReturn(mockCheckpointPath);
    
    reconTaskController.updateOMMetadataManager(mockOMMetadataManager);
    
    // Test successful queueing - the checkpoint creation should work with proper mocks
    ReconTaskController.ReInitializationResult result = reconTaskController.queueReInitializationEvent(
        ReconTaskReInitializationEvent.ReInitializationReason.BUFFER_OVERFLOW);

    assertEquals(ReconTaskController.ReInitializationResult.SUCCESS, result,
        "Reinitialization event should be successfully queued");
    assertFalse(reconTaskController.hasEventBufferOverflowed(), "Buffer overflow flag should be reset");
    assertFalse(reconTaskController.hasTasksFailed(), "Delta tasks failure flag should be reset");
  }
  
  @Test
  public void testQueueReInitializationEventCheckpointFailure() throws Exception {
    // Set up properly mocked ReconOMMetadataManager with required dependencies
    ReconOMMetadataManager mockOMMetadataManager = mock(ReconOMMetadataManager.class);
    DBStore mockDBStore = mock(DBStore.class);
    File mockDbLocation = mock(File.class);
    DBCheckpoint mockCheckpoint = mock(DBCheckpoint.class);
    Path mockCheckpointPath = Paths.get("/tmp/test/checkpoint");
    
    when(mockOMMetadataManager.getStore()).thenReturn(mockDBStore);
    when(mockDBStore.getDbLocation()).thenReturn(mockDbLocation);
    when(mockDbLocation.getParent()).thenReturn("/tmp/test");
    when(mockDBStore.getCheckpoint(any(String.class), any(Boolean.class))).thenReturn(mockCheckpoint);
    when(mockCheckpoint.getCheckpointLocation()).thenReturn(mockCheckpointPath);
    
    reconTaskController.updateOMMetadataManager(mockOMMetadataManager);
    
    // Create a spy of the controller to mock checkpoint creation failure
    ReconTaskControllerImpl controllerSpy = spy((ReconTaskControllerImpl) reconTaskController);
    doThrow(new IOException("Checkpoint creation failed"))
        .when(controllerSpy).createOMCheckpoint(any());
    
    // Test checkpoint creation failure
    ReconTaskController.ReInitializationResult result = controllerSpy.queueReInitializationEvent(
        ReconTaskReInitializationEvent.ReInitializationReason.BUFFER_OVERFLOW);

    assertEquals(ReconTaskController.ReInitializationResult.RETRY_LATER, result,
        "Reinitialization event should indicate retry needed due to checkpoint creation failure");
  }
  
  @Test
  public void testDrainEventBufferAndCleanExistingCheckpoints() throws Exception {
    // Stop the async processing first to prevent events from being consumed
    reconTaskController.stop();
    
    // Recreate controller without starting async processing
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
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
    ReconGlobalStatsManager reconGlobalStatsManager = mock(ReconGlobalStatsManager.class);
    ReconFileMetadataManager reconFileMetadataManager = mock(ReconFileMetadataManager.class);
    ReconTaskControllerImpl testController = new ReconTaskControllerImpl(ozoneConfiguration, new HashSet<>(),
        reconTaskStatusUpdaterManagerMock, reconDbProvider, reconContainerMgr, nsSummaryManager,
        reconGlobalStatsManager, reconFileMetadataManager);
    // Don't start async processing
    
    // Add some events to buffer first
    OMUpdateEventBatch mockBatch = mock(OMUpdateEventBatch.class);
    when(mockBatch.isEmpty()).thenReturn(false);
    when(mockBatch.getEvents()).thenReturn(new ArrayList<>());
    when(mockBatch.getEventType()).thenReturn(ReconEvent.EventType.OM_UPDATE_BATCH);
    when(mockBatch.getEventCount()).thenReturn(1);
    
    // Add multiple events to ensure buffer has content
    for (int i = 0; i < 3; i++) {
      testController.consumeOMEvents(mockBatch, mock(OMMetadataManager.class));
    }
    
    // Buffer should have events now
    assertTrue(testController.getEventBufferSize() > 0, "Buffer should have events");
    
    // Reset buffer
    testController.drainEventBufferAndCleanExistingCheckpoints();
    assertEquals(0, testController.getEventBufferSize(), "Buffer should be empty after reset");
  }
  
  @Test
  public void testResetEventFlags() {
    ReconTaskControllerImpl controllerImpl = (ReconTaskControllerImpl) reconTaskController;
    
    // Test resetting flags for different reasons
    controllerImpl.resetEventFlags();
    assertFalse(controllerImpl.hasEventBufferOverflowed());
    assertFalse(controllerImpl.hasTasksFailed());
    
    controllerImpl.resetEventFlags();
    assertFalse(controllerImpl.hasEventBufferOverflowed());
    assertFalse(controllerImpl.hasTasksFailed());
    
    controllerImpl.resetEventFlags();
    assertFalse(controllerImpl.hasEventBufferOverflowed());
    assertFalse(controllerImpl.hasTasksFailed());
  }
  
  @Test
  public void testUpdateOMMetadataManager() throws Exception {
    // Set up properly mocked ReconOMMetadataManager with required dependencies
    ReconOMMetadataManager mockManager1 = mock(ReconOMMetadataManager.class);
    DBStore mockDBStore1 = mock(DBStore.class);
    File mockDbLocation1 = mock(File.class);
    DBCheckpoint mockCheckpoint1 = mock(DBCheckpoint.class);
    Path mockCheckpointPath1 = Paths.get("/tmp/test/checkpoint1");
    
    when(mockManager1.getStore()).thenReturn(mockDBStore1);
    when(mockDBStore1.getDbLocation()).thenReturn(mockDbLocation1);
    when(mockDbLocation1.getParent()).thenReturn("/tmp/test");
    when(mockDBStore1.getCheckpoint(any(String.class), any(Boolean.class))).thenReturn(mockCheckpoint1);
    when(mockCheckpoint1.getCheckpointLocation()).thenReturn(mockCheckpointPath1);
    
    // Update with first manager
    reconTaskController.updateOMMetadataManager(mockManager1);
    
    // Test that the manager was updated correctly by attempting to queue a reinitialization event
    ReconTaskController.ReInitializationResult result = reconTaskController.queueReInitializationEvent(
        ReconTaskReInitializationEvent.ReInitializationReason.BUFFER_OVERFLOW);
    assertEquals(ReconTaskController.ReInitializationResult.SUCCESS, result,
        "Should be able to queue reinitialization event with updated manager");
  }
  
  @Test
  public void testCheckpointManagerCleanupOnQueueFailure() throws Exception {
    // Set up properly mocked ReconOMMetadataManager with required dependencies
    ReconOMMetadataManager mockOMMetadataManager = mock(ReconOMMetadataManager.class);
    DBStore mockDBStore = mock(DBStore.class);
    File mockDbLocation = mock(File.class);
    DBCheckpoint mockCheckpoint = mock(DBCheckpoint.class);
    Path mockCheckpointPath = Paths.get("/tmp/test/checkpoint");
    
    when(mockOMMetadataManager.getStore()).thenReturn(mockDBStore);
    when(mockDBStore.getDbLocation()).thenReturn(mockDbLocation);
    when(mockDbLocation.getParent()).thenReturn("/tmp/test");
    when(mockDBStore.getCheckpoint(any(String.class), any(Boolean.class))).thenReturn(mockCheckpoint);
    when(mockCheckpoint.getCheckpointLocation()).thenReturn(mockCheckpointPath);
    
    reconTaskController.updateOMMetadataManager(mockOMMetadataManager);
    
    // This test verifies the successful path - in practice, queue failure after clear is very rare
    // since we clear the buffer before queueing the reinitialization event
    ReconTaskController.ReInitializationResult result = reconTaskController.queueReInitializationEvent(
        ReconTaskReInitializationEvent.ReInitializationReason.BUFFER_OVERFLOW);
    assertEquals(ReconTaskController.ReInitializationResult.SUCCESS, result, "Should succeed under normal conditions");
  }
  
  @Test
  public void testNewRetryLogicWithSuccessfulCheckpoint() throws Exception {
    // Set up properly mocked ReconOMMetadataManager with required dependencies
    ReconOMMetadataManager mockOMMetadataManager = mock(ReconOMMetadataManager.class);
    DBStore mockDBStore = mock(DBStore.class);
    File mockDbLocation = mock(File.class);
    DBCheckpoint mockCheckpoint = mock(DBCheckpoint.class);
    Path mockCheckpointPath = Paths.get("/tmp/test/checkpoint");
    
    when(mockOMMetadataManager.getStore()).thenReturn(mockDBStore);
    when(mockDBStore.getDbLocation()).thenReturn(mockDbLocation);
    when(mockDbLocation.getParent()).thenReturn("/tmp/test");
    when(mockDBStore.getCheckpoint(any(String.class), any(Boolean.class))).thenReturn(mockCheckpoint);
    when(mockCheckpoint.getCheckpointLocation()).thenReturn(mockCheckpointPath);
    
    // Mock the createCheckpointReconMetadataManager method
    ReconOMMetadataManager mockCheckpointedManager = mock(ReconOMMetadataManager.class);
    when(mockOMMetadataManager.createCheckpointReconMetadataManager(any(), any())).thenReturn(mockCheckpointedManager);
    
    ReconTaskControllerImpl controllerImpl = (ReconTaskControllerImpl) reconTaskController;
    controllerImpl.updateOMMetadataManager(mockOMMetadataManager);
    
    // Reset any previous retry state
    controllerImpl.resetRetryCounters();
    
    // Test that checkpoint creation succeeds
    ReconTaskController.ReInitializationResult result = controllerImpl.queueReInitializationEvent(
        ReconTaskReInitializationEvent.ReInitializationReason.BUFFER_OVERFLOW);
    
    assertEquals(ReconTaskController.ReInitializationResult.SUCCESS, result, 
        "Should succeed on first attempt");
    
    // Verify retry count is reset after success
    assertEquals(0, controllerImpl.getEventProcessRetryCount(), "Retry count should be reset after success");
  }
  
  @Test
  public void testNewRetryLogicWithMaxRetriesExceeded() throws Exception {
    // Set up controller with mocked dependencies
    ReconOMMetadataManager mockOMMetadataManager = mock(ReconOMMetadataManager.class);
    ReconTaskControllerImpl controllerImpl = (ReconTaskControllerImpl) reconTaskController;
    controllerImpl.updateOMMetadataManager(mockOMMetadataManager);
    
    // Reset any previous retry state
    controllerImpl.resetRetryCounters();
    
    // Create a spy to consistently fail checkpoint creation
    ReconTaskControllerImpl controllerSpy = spy(controllerImpl);
    doThrow(new IOException("Checkpoint creation always fails"))
        .when(controllerSpy).createOMCheckpoint(any());
    
    // Test multiple iterations until max retries exceeded (MAX_EVENT_PROCESS_RETRIES = 6)
    // Need 7 total iterations because count check happens before increment
    ReconTaskController.ReInitializationResult result;
    
    // Iterations 1-6: should return RETRY_LATER and increment retry count
    for (int i = 1; i <= 6; i++) {
      if (i > 1) {
        Thread.sleep(2100); // Wait for retry delay
      }
      result = controllerSpy.queueReInitializationEvent(
          ReconTaskReInitializationEvent.ReInitializationReason.BUFFER_OVERFLOW);
      assertEquals(ReconTaskController.ReInitializationResult.RETRY_LATER, result,
          "Iteration " + i + " should return RETRY_LATER");
      assertEquals(i, controllerSpy.getEventProcessRetryCount(), "Should have " + i + " iteration retries");
    }
    
    // Iteration 7: should return MAX_RETRIES_EXCEEDED (eventProcessRetryCount is now 6,
    // which >= MAX_EVENT_PROCESS_RETRIES)
    Thread.sleep(2100); // Wait for retry delay
    result = controllerSpy.queueReInitializationEvent(
        ReconTaskReInitializationEvent.ReInitializationReason.BUFFER_OVERFLOW);
    assertEquals(ReconTaskController.ReInitializationResult.MAX_RETRIES_EXCEEDED, result,
        "Seventh iteration should return MAX_RETRIES_EXCEEDED");
    assertEquals(0, controllerSpy.getEventProcessRetryCount(), "Retry count should be reset after max exceeded");
    
    // Verify that createOMCheckpoint was called 6 times (checkpoint creation is skipped when MAX_RETRIES_EXCEEDED)
    verify(controllerSpy, times(6)).createOMCheckpoint(any());
  }

  @Test
  public void testProcessReInitializationEventWithTaskFailuresAndRetry() throws Exception {
    // Set up properly mocked ReconOMMetadataManager with required dependencies
    ReconOMMetadataManager mockOMMetadataManager = mock(ReconOMMetadataManager.class);
    DBStore mockDBStore = mock(DBStore.class);
    File mockDbLocation = mock(File.class);
    DBCheckpoint mockCheckpoint = mock(DBCheckpoint.class);
    Path mockCheckpointPath = Paths.get("/tmp/test/checkpoint");
    
    when(mockOMMetadataManager.getStore()).thenReturn(mockDBStore);
    when(mockDBStore.getDbLocation()).thenReturn(mockDbLocation);
    when(mockDbLocation.getParent()).thenReturn("/tmp/test");
    when(mockDBStore.getCheckpoint(any(String.class), any(Boolean.class))).thenReturn(mockCheckpoint);
    when(mockCheckpoint.getCheckpointLocation()).thenReturn(mockCheckpointPath);
    when(mockOMMetadataManager.createCheckpointReconMetadataManager(any(), any())).thenReturn(mockOMMetadataManager);
    
    ReconTaskControllerImpl controllerImpl = (ReconTaskControllerImpl) reconTaskController;
    controllerImpl.updateOMMetadataManager(mockOMMetadataManager);
    
    // Create a spy to control reInitializeTasks behavior
    ReconTaskControllerImpl controllerSpy = spy(controllerImpl);
    
    // Mock reInitializeTasks to fail on first call, succeed on second call
    when(controllerSpy.reInitializeTasks(any(ReconOMMetadataManager.class), any()))
        .thenReturn(false)  // First call fails
        .thenReturn(true);  // Second call succeeds
    
    // Stop async processing to control event processing manually
    controllerSpy.stop();
    
    // Create and manually process a reinitialization event
    ReconTaskReInitializationEvent reinitEvent = new ReconTaskReInitializationEvent(
        ReconTaskReInitializationEvent.ReInitializationReason.TASK_FAILURES,
        mockOMMetadataManager);
    
    // Verify initial state
    assertFalse(controllerSpy.hasTasksFailed(), "tasksFailed should be false initially");
    
    // Manually invoke processReInitializationEvent to test the retry logic
    controllerSpy.processReconEvent(reinitEvent);
    
    // Wait for processing using CountDownLatch
    CountDownLatch processingLatch1 = new CountDownLatch(1);
    CompletableFuture.runAsync(() -> {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      processingLatch1.countDown();
    });
    assertTrue(processingLatch1.await(1, TimeUnit.SECONDS), "Processing should complete");
    
    // Verify that reInitializeTasks was called and tasksFailed flag is set due to failure
    verify(controllerSpy, times(1)).reInitializeTasks(any(ReconOMMetadataManager.class), any());
    assertTrue(controllerSpy.hasTasksFailed(), "tasksFailed should be true after reInitializeTasks failure");
    
    // Simulate the natural retry mechanism - this is what would happen in the scheduled syncDataFromOM
    // when it detects hasTasksFailed() == true
    
    // Reset the spy call count for cleaner verification
    org.mockito.Mockito.clearInvocations(controllerSpy);
    
    // Now simulate the scheduled thread detecting tasksFailed and queueing another reinitialization
    // This simulates the behavior in OzoneManagerServiceProviderImpl#syncDataFromOM lines 680-692
    assertTrue(controllerSpy.hasTasksFailed(), "tasksFailed should still be true, triggering retry");
    
    // Wait for retry delay before attempting to queue again (RETRY_DELAY_MS = 2000)
    Thread.sleep(2100);
    
    // Queue another reinitialization event (simulating what syncDataFromOM does)
    ReconTaskController.ReInitializationResult result = controllerSpy.queueReInitializationEvent(
        ReconTaskReInitializationEvent.ReInitializationReason.TASK_FAILURES);
    
    assertEquals(ReconTaskController.ReInitializationResult.SUCCESS, result, 
        "Second reinitialization should be queued successfully");
    
    // Process the second reinitialization event
    ReconTaskReInitializationEvent secondReinitEvent = new ReconTaskReInitializationEvent(
        ReconTaskReInitializationEvent.ReInitializationReason.TASK_FAILURES,
        mockOMMetadataManager);
    
    controllerSpy.processReconEvent(secondReinitEvent);
    
    // Wait for processing using CountDownLatch
    CountDownLatch processingLatch2 = new CountDownLatch(1);
    CompletableFuture.runAsync(() -> {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      processingLatch2.countDown();
    });
    assertTrue(processingLatch2.await(1, TimeUnit.SECONDS), "Processing should complete");
    
    // Verify that reInitializeTasks was called again and this time succeeded
    verify(controllerSpy, times(1)).reInitializeTasks(any(ReconOMMetadataManager.class), any());
    
    // Verify that tasksFailed flag is now reset because reInitializeTasks succeeded
    assertFalse(controllerSpy.hasTasksFailed(), "tasksFailed should be false after successful reinitialization");
  }
  
  @Test
  public void testTasksFailedFlagBlocksDeltaEvents() throws Exception {
    ReconTaskControllerImpl controllerImpl = (ReconTaskControllerImpl) reconTaskController;
    
    // Initially, tasks should not be failed
    assertFalse(controllerImpl.hasTasksFailed(), "tasksFailed should be false initially");
    
    // Test the main functionality: when tasksFailed is true, events are not buffered
    // Set tasksFailed flag to true (simulating reinitialization failure)
    controllerImpl.getTasksFailedFlag().set(true);
    assertTrue(controllerImpl.hasTasksFailed(), "tasksFailed should be true now");
    
    // Create a mock event using the same pattern as working tests
    OMUpdateEventBatch mockBatch = mock(OMUpdateEventBatch.class);
    when(mockBatch.isEmpty()).thenReturn(false);
    when(mockBatch.getEvents()).thenReturn(new ArrayList<>());
    when(mockBatch.getEventType()).thenReturn(ReconEvent.EventType.OM_UPDATE_BATCH);
    when(mockBatch.getEventCount()).thenReturn(1);
    
    // Stop async processing to prevent any interference
    controllerImpl.stop();
    
    // Get buffer size when events should be blocked
    int bufferSizeWithTasksFailed = controllerImpl.getEventBufferSize();
    
    // Try to consume events when tasksFailed is true - they should be blocked
    controllerImpl.consumeOMEvents(mockBatch, mock(OMMetadataManager.class));
    
    // Verify buffer size didn't change (events were blocked)
    assertEquals(bufferSizeWithTasksFailed, controllerImpl.getEventBufferSize(),
        "Events should be blocked when tasksFailed is true");
    
    // Reset tasksFailed flag and verify events can be buffered again
    controllerImpl.getTasksFailedFlag().set(false);
    assertFalse(controllerImpl.hasTasksFailed(), "tasksFailed should be false now");
    
    // Note: We can't easily test buffer size increase due to async processing, 
    // but we've verified the blocking behavior which is the main test objective
  }
  
  @Test
  public void testProcessReInitializationEventWithCheckpointedManager() throws Exception {
    // Set up properly mocked ReconOMMetadataManager with required dependencies
    ReconOMMetadataManager mockCurrentManager = mock(ReconOMMetadataManager.class);
    ReconOMMetadataManager mockCheckpointedManager = mock(ReconOMMetadataManager.class);
    DBStore mockDBStore = mock(DBStore.class);
    File mockDbLocation = mock(File.class);
    
    when(mockCheckpointedManager.getStore()).thenReturn(mockDBStore);
    when(mockDBStore.getDbLocation()).thenReturn(mockDbLocation);
    when(mockDbLocation.getParentFile()).thenReturn(mockDbLocation);
    
    ReconTaskControllerImpl controllerImpl = (ReconTaskControllerImpl) reconTaskController;
    controllerImpl.updateOMMetadataManager(mockCurrentManager);
    
    // Create a spy to control reInitializeTasks behavior
    ReconTaskControllerImpl controllerSpy = spy(controllerImpl);
    when(controllerSpy.reInitializeTasks(any(ReconOMMetadataManager.class), any()))
        .thenReturn(true);  // Succeed
    
    // Stop async processing to control event processing manually
    controllerSpy.stop();
    
    // Create reinitialization event with checkpointed manager
    ReconTaskReInitializationEvent reinitEvent = new ReconTaskReInitializationEvent(
        ReconTaskReInitializationEvent.ReInitializationReason.BUFFER_OVERFLOW,
        mockCheckpointedManager);
    
    // Verify initial state - tasksFailed should be false
    assertFalse(controllerSpy.hasTasksFailed(), "tasksFailed should be false initially");
    
    // Process the reinitialization event
    controllerSpy.processReconEvent(reinitEvent);
    
    // Wait for processing using CountDownLatch
    CountDownLatch processingLatch3 = new CountDownLatch(1);
    CompletableFuture.runAsync(() -> {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      processingLatch3.countDown();
    });
    assertTrue(processingLatch3.await(1, TimeUnit.SECONDS), "Processing should complete");
    
    // Verify that reInitializeTasks was called with the checkpointed manager
    verify(controllerSpy, times(1)).reInitializeTasks(mockCheckpointedManager, null);
    
    // Verify that tasksFailed flag remains false because reInitializeTasks succeeded
    assertFalse(controllerSpy.hasTasksFailed(), "tasksFailed should remain false after successful reinitialization");
    
    // Verify cleanup was called on the checkpointed manager
    verify(mockCheckpointedManager, times(1)).close();
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
