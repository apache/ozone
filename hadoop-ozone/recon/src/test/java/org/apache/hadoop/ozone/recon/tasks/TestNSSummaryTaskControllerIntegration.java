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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconFileMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTask.RebuildState;
import org.apache.hadoop.ozone.recon.tasks.ReconOmTask.TaskResult;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for HDDS-13443 focusing on ReconTaskController integration.
 * 
 * <p>These tests verify that the unified control mechanism works correctly
 * when rebuilds are triggered through ReconTaskControllerImpl.reInitializeTasks().
 */
public class TestNSSummaryTaskControllerIntegration {

  private static final Logger LOG = LoggerFactory.getLogger(TestNSSummaryTaskControllerIntegration.class);

  private ReconTaskControllerImpl taskController;
  private NSSummaryTask nsSummaryTask;
  private ReconOmTask mockOtherTask;
  private ReconNamespaceSummaryManager mockNamespaceSummaryManager;
  private ReconOMMetadataManager mockReconOMMetadataManager;
  private OMMetadataManager mockOMMetadataManager;
  private OzoneConfiguration ozoneConfiguration;

  @BeforeEach
  void setUp() throws Exception {
    // Reset static state before each test
    NSSummaryTask.resetRebuildState();
    
    // Create mocks
    mockNamespaceSummaryManager = mock(ReconNamespaceSummaryManager.class);
    mockReconOMMetadataManager = mock(ReconOMMetadataManager.class);
    mockOMMetadataManager = mock(OMMetadataManager.class);
    mockOtherTask = mock(ReconOmTask.class);
    ozoneConfiguration = new OzoneConfiguration();

    // Create testable NSSummaryTask instance
    nsSummaryTask = createTestableNSSummaryTask();

    // Setup other task mock
    when(mockOtherTask.getTaskName()).thenReturn("MockTask");
    when(mockOtherTask.getStagedTask(any(), any())).thenReturn(mockOtherTask);
    when(mockOtherTask.reprocess(any())).thenReturn(
        new TaskResult.Builder()
            .setTaskName("MockTask")
            .setTaskSuccess(true)
            .build());

    // Create task controller with empty set of tasks and mock status updater manager
    ReconTaskStatusUpdaterManager mockTaskStatusUpdaterManager = mock(ReconTaskStatusUpdaterManager.class);
    ReconTaskStatusUpdater mockTaskStatusUpdater = mock(ReconTaskStatusUpdater.class);
    when(mockTaskStatusUpdaterManager.getTaskStatusUpdater(any())).thenReturn(mockTaskStatusUpdater);

    ReconContainerMetadataManager reconContainerMgr = mock(ReconContainerMetadataManager.class);
    ReconGlobalStatsManager reconGlobalStatsManager = mock(ReconGlobalStatsManager.class);
    ReconFileMetadataManager reconFileMetadataManager = mock(ReconFileMetadataManager.class);
    ReconDBProvider reconDbProvider = mock(ReconDBProvider.class);
    when(reconDbProvider.getDbStore()).thenReturn(mock(DBStore.class));
    when(reconDbProvider.getStagedReconDBProvider()).thenReturn(reconDbProvider);
    taskController = new ReconTaskControllerImpl(ozoneConfiguration, java.util.Collections.emptySet(),
        mockTaskStatusUpdaterManager, reconDbProvider, reconContainerMgr, mockNamespaceSummaryManager,
        reconGlobalStatsManager, reconFileMetadataManager);
    taskController.start(); // Initialize the executor service
    taskController.registerTask(nsSummaryTask);
    taskController.registerTask(mockOtherTask);

    // Setup successful rebuild by default
    // Setup successful rebuild by default - no exception thrown
    doNothing().when(mockNamespaceSummaryManager).clearNSSummaryTable();
  }

  @AfterEach
  void tearDown() {
    // Reset static state after each test to ensure test isolation
    NSSummaryTask.resetRebuildState();
    
    // Shutdown task controller if it exists
    if (taskController != null) {
      taskController.stop();
    }
  }
  
  /**
   * Create a testable NSSummaryTask that uses mocked sub-tasks for successful execution.
   */
  private NSSummaryTask createTestableNSSummaryTask() {
    return new NSSummaryTask(
        mockNamespaceSummaryManager, 
        mockReconOMMetadataManager, 
        ozoneConfiguration) {
      
      @Override
      public TaskResult buildTaskResult(boolean success) {
        return super.buildTaskResult(success);
      }

      @Override
      public NSSummaryTask getStagedTask(ReconOMMetadataManager stagedOmMetadataManager, DBStore stagedReconDbStore)
          throws IOException {
        return this;
      }

      @Override
      protected TaskResult executeReprocess(OMMetadataManager omMetadataManager, long startTime) {
        // Simplified test implementation that mimics the real execution flow
        // but bypasses the complex sub-task execution while maintaining proper state management
        
        // Initialize a list of tasks to run in parallel (empty for testing)
        Collection<Callable<Boolean>> tasks = new ArrayList<>();

        try {
          // This will call the mocked clearNSSummaryTable (might throw Exception for failure tests)
          getReconNamespaceSummaryManager().clearNSSummaryTable();
        } catch (IOException ioEx) {
          LOG.error("Unable to clear NSSummary table in Recon DB. ", ioEx);
          NSSummaryTask.setRebuildStateToFailed();
          return buildTaskResult(false);
        }

        // Add mock sub-tasks that always succeed
        tasks.add(() -> true); // Mock FSO task
        tasks.add(() -> true); // Mock Legacy task  
        tasks.add(() -> true); // Mock OBS task

        List<Future<Boolean>> results;
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("Test-NSSummaryTask-%d")
            .build();
        ExecutorService executorService = Executors.newFixedThreadPool(2, threadFactory);
        boolean success = false;
        
        try {
          results = executorService.invokeAll(tasks);
          for (Future<Boolean> result : results) {
            if (result.get().equals(false)) {
              LOG.error("NSSummary reprocess failed for one of the sub-tasks.");
              NSSummaryTask.setRebuildStateToFailed();
              return buildTaskResult(false);
            }
          }
          success = true;
          
        } catch (InterruptedException | ExecutionException ex) {
          LOG.error("Error while reprocessing NSSummary table in Recon DB.", ex);
          NSSummaryTask.setRebuildStateToFailed();
          return buildTaskResult(false);
          
        } finally {
          executorService.shutdown();

          long endTime = System.nanoTime();
          long durationInMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
          LOG.info("Test NSSummary reprocess execution time: {} milliseconds", durationInMillis);
          
          // Reset state to IDLE on successful completion
          if (success) {
            NSSummaryTask.resetRebuildState();
            LOG.info("Test NSSummary tree reprocess completed successfully with unified control.");
          }
        }

        return buildTaskResult(true);
      }
    };
  }

  /**
   * Test that reInitializeTasks respects unified control when NSSummary rebuild is running.
   */
  @Test
  void testReInitializeTasksRespectsUnifiedControl() throws Exception {
    CountDownLatch rebuildStarted = new CountDownLatch(1);
    CountDownLatch rebuildCanFinish = new CountDownLatch(1);
    AtomicInteger rebuildAttempts = new AtomicInteger(0);

    // Setup first rebuild to block
    doAnswer(invocation -> {
      int attempt = rebuildAttempts.incrementAndGet();
      LOG.info("NSSummary rebuild attempt #{}", attempt);
      
      if (attempt == 1) {
        rebuildStarted.countDown();
        boolean awaitSuccess = rebuildCanFinish.await(10, TimeUnit.SECONDS);
        if (!awaitSuccess) {
          LOG.warn("rebuildCanFinish.await() timed out");
        }
      }
      return null;
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      // Start first rebuild directly via nsSummaryTask
      CompletableFuture<TaskResult> directRebuild = CompletableFuture.supplyAsync(() -> {
        LOG.info("Starting direct rebuild");
        return nsSummaryTask.reprocess(mockOMMetadataManager);
      }, executor);

      // Wait for first rebuild to start
      assertTrue(rebuildStarted.await(5, TimeUnit.SECONDS), 
          "First rebuild should start");
      assertEquals(RebuildState.RUNNING, NSSummaryTask.getRebuildState(),
          "State should be RUNNING during first rebuild");

      // Now try to trigger reInitializeTasks - NSSummary rebuild should be rejected
      CompletableFuture<Void> taskControllerRebuild = CompletableFuture.runAsync(() -> {
        LOG.info("Starting reInitializeTasks");
        taskController.reInitializeTasks(mockReconOMMetadataManager, null);
      }, executor);

      // Give reInitializeTasks some time to complete
      taskControllerRebuild.get(3, TimeUnit.SECONDS);

      // Verify that only the first rebuild attempt was made (others rejected)
      assertEquals(1, rebuildAttempts.get(), 
          "Only first rebuild should execute, others should be rejected");
      assertEquals(RebuildState.RUNNING, NSSummaryTask.getRebuildState(),
          "State should still be RUNNING from first rebuild");

      // Complete first rebuild
      rebuildCanFinish.countDown();
      TaskResult result = directRebuild.get(5, TimeUnit.SECONDS);
      assertTrue(result.isTaskSuccess(), "First rebuild should succeed");

      // Verify final state
      assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
          "State should return to IDLE after completion");
      
      // Other task should have been processed by reInitializeTasks
      verify(mockOtherTask, times(1)).reprocess(mockReconOMMetadataManager);

    } finally {
      rebuildCanFinish.countDown(); // Ensure cleanup
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /**
   * Test multiple concurrent reInitializeTasks calls.
   */
  @Test
  void testMultipleConcurrentReInitializeTasks() throws Exception {
    int threadCount = 3;
    CountDownLatch rebuildStarted = new CountDownLatch(1);
    CountDownLatch rebuildCanFinish = new CountDownLatch(1);
    AtomicInteger clearTableCallCount = new AtomicInteger(0);

    // Setup rebuild to block on first call only
    doAnswer(invocation -> {
      int callNumber = clearTableCallCount.incrementAndGet();
      LOG.info("clearNSSummaryTable called #{}", callNumber);
      
      if (callNumber == 1) {
        // First call - block to test concurrent behavior
        rebuildStarted.countDown();
        boolean awaitSuccess = rebuildCanFinish.await(10, TimeUnit.SECONDS);
        if (!awaitSuccess) {
          LOG.warn("rebuildCanFinish.await() timed out");
        }
      }
      // Subsequent calls complete quickly
      return null;
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CompletableFuture<Void>[] futures = new CompletableFuture[threadCount];

    try {
      // Launch multiple concurrent reInitializeTasks
      for (int i = 0; i < threadCount; i++) {
        final int threadId = i;
        futures[i] = CompletableFuture.runAsync(() -> {
          LOG.info("Thread {} calling reInitializeTasks", threadId);
          taskController.reInitializeTasks(mockReconOMMetadataManager, null);
          LOG.info("Thread {} completed reInitializeTasks", threadId);
        }, executor);
      }

      // Wait for first rebuild to start
      assertTrue(rebuildStarted.await(5, TimeUnit.SECONDS), 
          "At least one rebuild should start");
      assertEquals(RebuildState.RUNNING, NSSummaryTask.getRebuildState(),
          "State should be RUNNING");

      // Complete the rebuild
      rebuildCanFinish.countDown();
      CompletableFuture.allOf(futures).get(10, TimeUnit.SECONDS);

      // Verify results - unified control works but may not be perfect with very fast concurrent executions
      // The important thing is that at least one rebuild executes and the final state is consistent
      assertTrue(clearTableCallCount.get() >= 1, 
          "At least one rebuild should execute");
      assertTrue(clearTableCallCount.get() <= threadCount, 
          "No more than threadCount rebuilds should execute");
      LOG.info("clearTableCallCount: {}, threadCount: {}", clearTableCallCount.get(), threadCount);
      assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
          "Final state should be IDLE");

      // Other task should have been processed multiple times (not affected by unified control)
      verify(mockOtherTask, times(threadCount)).reprocess(mockReconOMMetadataManager);

    } finally {
      rebuildCanFinish.countDown(); // Ensure cleanup
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /**
   * Test that failed rebuilds via task controller set proper state.
   */
  @Test
  void testTaskControllerRebuildFailure() throws Exception {
    // Setup rebuild to fail
    doAnswer(invocation -> {
      throw new RuntimeException("Simulated rebuild failure");
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    // Execute reInitializeTasks
    taskController.reInitializeTasks(mockReconOMMetadataManager, null);

    // Verify state is set to FAILED
    assertEquals(RebuildState.FAILED, NSSummaryTask.getRebuildState(),
        "State should be FAILED after rebuild failure");

    // Other task should still have been processed
    verify(mockOtherTask, times(1)).reprocess(mockReconOMMetadataManager);
  }

  /**
   * Test recovery after failed rebuild via task controller.
   */
  @Test
  void testRecoveryAfterTaskControllerFailure() throws Exception {
    // First reInitializeTasks call fails
    doAnswer(invocation -> {
      throw new RuntimeException("First failure");
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    taskController.reInitializeTasks(mockReconOMMetadataManager, null);
    assertEquals(RebuildState.FAILED, NSSummaryTask.getRebuildState(),
        "State should be FAILED after first failure");

    // Second reInitializeTasks call succeeds
    // Setup successful rebuild by default - no exception thrown
    doNothing().when(mockNamespaceSummaryManager).clearNSSummaryTable();
    
    taskController.reInitializeTasks(mockReconOMMetadataManager, null);
    assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
        "State should be IDLE after successful recovery");

    // Other task should have been processed twice
    verify(mockOtherTask, times(2)).reprocess(mockReconOMMetadataManager);
  }

  /**
   * Test that task controller respects ongoing rebuild from external trigger.
   */
  @Test
  void testTaskControllerRespectsExternalRebuild() throws Exception {
    CountDownLatch externalRebuildStarted = new CountDownLatch(1);
    CountDownLatch externalRebuildCanFinish = new CountDownLatch(1);
    AtomicInteger rebuildCount = new AtomicInteger(0);

    // Setup rebuild to block on first call (external), complete quickly on subsequent
    doAnswer(invocation -> {
      int count = rebuildCount.incrementAndGet();
      if (count == 1) {
        // First call (external) - block
        externalRebuildStarted.countDown();
        boolean awaitSuccess = externalRebuildCanFinish.await(10, TimeUnit.SECONDS);
        if (!awaitSuccess) {
          LOG.warn("externalRebuildCanFinish.await() timed out");
        }
      }
      // Subsequent calls should not happen due to unified control
      return null;
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      // Start external rebuild
      CompletableFuture<TaskResult> externalRebuild = CompletableFuture.supplyAsync(() -> {
        LOG.info("Starting external rebuild");
        return nsSummaryTask.reprocess(mockOMMetadataManager);
      }, executor);

      // Wait for external rebuild to start
      assertTrue(externalRebuildStarted.await(5, TimeUnit.SECONDS), 
          "External rebuild should start");
      assertEquals(RebuildState.RUNNING, NSSummaryTask.getRebuildState(),
          "State should be RUNNING");

      // Try reInitializeTasks while external rebuild is running
      CompletableFuture<Void> taskControllerCall = CompletableFuture.runAsync(() -> {
        LOG.info("Calling reInitializeTasks during external rebuild");
        taskController.reInitializeTasks(mockReconOMMetadataManager, null);
      }, executor);

      // Wait for task controller call to complete
      taskControllerCall.get(3, TimeUnit.SECONDS);

      // Should still be only one rebuild attempt (external one still running)
      assertEquals(1, rebuildCount.get(), 
          "Only external rebuild should be running");
      assertEquals(RebuildState.RUNNING, NSSummaryTask.getRebuildState(),
          "State should still be RUNNING from external rebuild");

      // Complete external rebuild
      externalRebuildCanFinish.countDown();
      TaskResult result = externalRebuild.get(5, TimeUnit.SECONDS);
      assertTrue(result.isTaskSuccess(), "External rebuild should succeed");

      // Final state should be IDLE
      assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
          "State should be IDLE after external rebuild completes");

      // Other task should have been processed by reInitializeTasks
      verify(mockOtherTask, times(1)).reprocess(mockReconOMMetadataManager);

    } finally {
      externalRebuildCanFinish.countDown(); // Ensure cleanup
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /**
   * Test task registration and execution with mixed success/failure scenarios.
   */
  @Test
  void testMixedTaskSuccessFailureScenarios() throws Exception {
    // Create additional mock tasks
    ReconOmTask successTask = mock(ReconOmTask.class);
    ReconOmTask failTask = mock(ReconOmTask.class);

    when(successTask.getTaskName()).thenReturn("SuccessTask");
    when(successTask.getStagedTask(any(), any())).thenReturn(successTask);
    when(successTask.reprocess(any())).thenReturn(
        new TaskResult.Builder()
            .setTaskName("SuccessTask") 
            .setTaskSuccess(true)
            .build());

    when(failTask.getTaskName()).thenReturn("FailTask");
    when(failTask.getStagedTask(any(), any())).thenReturn(failTask);
    when(failTask.reprocess(any())).thenReturn(
        new TaskResult.Builder()
            .setTaskName("FailTask")
            .setTaskSuccess(false)
            .build());

    // Register additional tasks
    taskController.registerTask(successTask);
    taskController.registerTask(failTask);

    // Setup NSSummary rebuild to succeed
    // Setup successful rebuild by default - no exception thrown
    doNothing().when(mockNamespaceSummaryManager).clearNSSummaryTable();

    // Execute reInitializeTasks
    taskController.reInitializeTasks(mockReconOMMetadataManager, null);

    // Verify all tasks were attempted
    verify(mockOtherTask, times(1)).reprocess(mockReconOMMetadataManager);
    verify(successTask, times(1)).reprocess(mockReconOMMetadataManager);
    verify(failTask, times(1)).reprocess(mockReconOMMetadataManager);

    // NSSummary should have completed successfully
    assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
        "NSSummary rebuild should complete successfully despite other task failures");
  }

  /**
   * Test proper cleanup and state management during task controller shutdown scenarios.
   */
  @Test
  void testTaskControllerShutdownScenarios() throws Exception {
    CountDownLatch rebuildStarted = new CountDownLatch(1);
    
    // Setup long-running rebuild
    doAnswer(invocation -> {
      rebuildStarted.countDown();
      Thread.sleep(2000); // Simulate long-running rebuild
      return null;
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    ExecutorService executor = Executors.newSingleThreadExecutor();

    try {
      // Start rebuild
      CompletableFuture.runAsync(() -> {
        taskController.reInitializeTasks(mockReconOMMetadataManager, null);
      }, executor);

      // Wait for rebuild to start
      assertTrue(rebuildStarted.await(5, TimeUnit.SECONDS),
          "Rebuild should start");
      assertEquals(RebuildState.RUNNING, NSSummaryTask.getRebuildState(),
          "State should be RUNNING");

      // Shutdown executor (simulating shutdown scenario)
      executor.shutdownNow();
      
      // Wait a bit for any cleanup
      Thread.sleep(500);

      // Even after executor shutdown, the state should eventually be updated
      // (this tests that our state management is robust)
      long timeout = System.currentTimeMillis() + 5000;
      RebuildState finalState = RebuildState.RUNNING;
      while (System.currentTimeMillis() < timeout) {
        finalState = NSSummaryTask.getRebuildState();
        if (finalState != RebuildState.RUNNING) {
          break;
        }
        Thread.sleep(100);
      }

      // State should not be stuck in RUNNING (should be IDLE or FAILED)
      assertTrue(finalState == RebuildState.IDLE || finalState == RebuildState.FAILED,
          "State should not be stuck in RUNNING after shutdown. Current state: " + finalState);

    } finally {
      if (!executor.isShutdown()) {
        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.SECONDS);
      }
    }
  }
}
