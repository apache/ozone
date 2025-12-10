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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTask.RebuildState;
import org.apache.hadoop.ozone.recon.tasks.ReconOmTask.TaskResult;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for HDDS-13443: Unified and controlled sync access to 
 * retrigger of build of NSSummary tree.
 * 
 * <p>These tests verify that the unified control mechanism prevents concurrent
 * rebuilds and properly manages state transitions across all entry points.
 */
public class TestNSSummaryUnifiedControl {

  private static final Logger LOG = LoggerFactory.getLogger(TestNSSummaryUnifiedControl.class);

  private NSSummaryTask nsSummaryTask;
  private ReconNamespaceSummaryManager mockNamespaceSummaryManager;
  private ReconOMMetadataManager mockReconOMMetadataManager;
  private OMMetadataManager mockOMMetadataManager;
  private OzoneConfiguration ozoneConfiguration;

  @BeforeEach
  void setUp() throws IOException {
    // Reset static state before each test
    NSSummaryTask.resetRebuildState();
    
    // Create mocks
    mockNamespaceSummaryManager = mock(ReconNamespaceSummaryManager.class);
    mockReconOMMetadataManager = mock(ReconOMMetadataManager.class);
    mockOMMetadataManager = mock(OMMetadataManager.class);
    ozoneConfiguration = new OzoneConfiguration();

    // Create NSSummaryTask instance that will use mocked sub-tasks
    nsSummaryTask = createTestableNSSummaryTask();
  }

  @AfterEach
  void tearDown() {
    // Reset static state after each test to ensure test isolation
    NSSummaryTask.resetRebuildState();
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
   * Test that initial state is IDLE.
   */
  @Test
  void testInitialState() {
    assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
        "Initial rebuild state should be IDLE");
  }

  /**
   * Test successful single rebuild operation.
   */
  @Test
  void testSingleSuccessfulRebuild() throws Exception {
    // Setup successful rebuild
    // Setup successful rebuild by default - no exception thrown
    doNothing().when(mockNamespaceSummaryManager).clearNSSummaryTable();

    // Execute rebuild
    TaskResult result = nsSummaryTask.reprocess(mockOMMetadataManager);

    // Verify results
    assertTrue(result.isTaskSuccess(), "Rebuild should succeed");
    assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
        "State should return to IDLE after successful rebuild");
    
    // Verify interactions
    verify(mockNamespaceSummaryManager, times(1)).clearNSSummaryTable();
  }

  /**
   * Test rebuild failure sets state to FAILED.
   */
  @Test
  void testRebuildFailure() throws IOException {
    // Setup failure scenario
    doThrow(new IOException("Test failure")).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    // Execute rebuild
    TaskResult result = nsSummaryTask.reprocess(mockOMMetadataManager);

    // Verify results
    assertFalse(result.isTaskSuccess(), "Rebuild should fail");
    assertEquals(RebuildState.FAILED, NSSummaryTask.getRebuildState(),
        "State should be FAILED after rebuild failure");
  }

  /**
   * Test concurrent rebuild attempts - second call should be rejected.
   */
  @Test
  void testConcurrentRebuildPrevention() throws Exception {
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(1);
    AtomicBoolean firstRebuildStarted = new AtomicBoolean(false);
    AtomicBoolean secondRebuildRejected = new AtomicBoolean(false);

    // Setup first rebuild to block until we signal
    doAnswer(invocation -> {
      firstRebuildStarted.set(true);
      startLatch.countDown();
      // Wait for test to signal completion
      boolean awaitSuccess = finishLatch.await(10, TimeUnit.SECONDS);
      if (!awaitSuccess) {
        LOG.warn("finishLatch.await() timed out");
      }
      return null;
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      // Start first rebuild asynchronously
      CompletableFuture<TaskResult> firstRebuild = CompletableFuture.supplyAsync(() -> {
        LOG.info("Starting first rebuild");
        return nsSummaryTask.reprocess(mockOMMetadataManager);
      }, executor);

      // Wait for first rebuild to start
      assertTrue(startLatch.await(5, TimeUnit.SECONDS), 
          "First rebuild should start within timeout");
      assertTrue(firstRebuildStarted.get(), "First rebuild should have started");
      assertEquals(RebuildState.RUNNING, NSSummaryTask.getRebuildState(),
          "State should be RUNNING during first rebuild");

      // Attempt second rebuild - should be rejected immediately
      CompletableFuture<TaskResult> secondRebuild = CompletableFuture.supplyAsync(() -> {
        LOG.info("Attempting second rebuild");
        TaskResult result = nsSummaryTask.reprocess(mockOMMetadataManager);
        secondRebuildRejected.set(!result.isTaskSuccess());
        return result;
      }, executor);

      // Get second rebuild result quickly (should be immediate rejection)
      TaskResult secondResult = secondRebuild.get(2, TimeUnit.SECONDS);
      assertFalse(secondResult.isTaskSuccess(), 
          "Second rebuild should be rejected");
      assertTrue(secondRebuildRejected.get(), "Second rebuild should have been rejected");

      // Signal first rebuild to complete
      finishLatch.countDown();
      TaskResult firstResult = firstRebuild.get(5, TimeUnit.SECONDS);
      assertTrue(firstResult.isTaskSuccess(), "First rebuild should succeed");

      // Verify final state
      assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
          "State should return to IDLE after first rebuild completes");

    } finally {
      finishLatch.countDown(); // Ensure cleanup
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /**
   * Test that rebuild can be triggered again after failure.
   */
  @Test
  void testRebuildAfterFailure() throws Exception {
    // First rebuild fails
    doThrow(new IOException("Test failure")).when(mockNamespaceSummaryManager).clearNSSummaryTable();
    
    TaskResult failedResult = nsSummaryTask.reprocess(mockOMMetadataManager);
    assertFalse(failedResult.isTaskSuccess(), "First rebuild should fail");
    assertEquals(RebuildState.FAILED, NSSummaryTask.getRebuildState(),
        "State should be FAILED after first rebuild");

    // Second rebuild succeeds
    // Setup successful rebuild by default - no exception thrown
    doNothing().when(mockNamespaceSummaryManager).clearNSSummaryTable();
    
    TaskResult successResult = nsSummaryTask.reprocess(mockOMMetadataManager);
    assertTrue(successResult.isTaskSuccess(), "Second rebuild should succeed");
    assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
        "State should be IDLE after successful rebuild");
  }

  /**
   * Test multiple concurrent attempts - only one should succeed, others rejected.
   */
  @Test
  @Flaky("HDDS-13573")
  void testMultipleConcurrentAttempts() throws Exception {
    int threadCount = 5;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(1);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger rejectedCount = new AtomicInteger(0);
    AtomicInteger clearTableCallCount = new AtomicInteger(0);

    // Ensure clean initial state
    NSSummaryTask.resetRebuildState();
    assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(), 
        "Initial state must be IDLE");

    // Setup rebuild to block and count calls
    doAnswer(invocation -> {
      int callNum = clearTableCallCount.incrementAndGet();
      LOG.info("clearNSSummaryTable called #{}, current state: {}", callNum, NSSummaryTask.getRebuildState());
      
      if (callNum == 1) {
        startLatch.countDown();
        boolean awaitSuccess = finishLatch.await(10, TimeUnit.SECONDS);
        if (!awaitSuccess) {
          LOG.warn("finishLatch.await() timed out");
        }
      }
      return null;
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CompletableFuture<Void>[] futures = new CompletableFuture[threadCount];

    try {
      // Launch multiple concurrent rebuilds
      for (int i = 0; i < threadCount; i++) {
        final int threadId = i;
        futures[i] = CompletableFuture.runAsync(() -> {
          LOG.info("Thread {} attempting rebuild, current state: {}", threadId, NSSummaryTask.getRebuildState());
          TaskResult result = nsSummaryTask.reprocess(mockOMMetadataManager);
          if (result.isTaskSuccess()) {
            int count = successCount.incrementAndGet();
            LOG.info("Thread {} rebuild succeeded (success #{})", threadId, count);
          } else {
            int count = rejectedCount.incrementAndGet();
            LOG.info("Thread {} rebuild rejected (rejection #{})", threadId, count);
          }
        }, executor);
      }

      // Wait for first rebuild to start
      assertTrue(startLatch.await(5, TimeUnit.SECONDS), 
          "At least one rebuild should start");
      assertEquals(RebuildState.RUNNING, NSSummaryTask.getRebuildState(),
          "State should be RUNNING");

      // Let rebuilds complete
      finishLatch.countDown();
      CompletableFuture.allOf(futures).get(10, TimeUnit.SECONDS);

      // Debug output
      LOG.info("Final counts - Success: {}, Rejected: {}, ClearTable calls: {}, Final state: {}", 
          successCount.get(), rejectedCount.get(), clearTableCallCount.get(), NSSummaryTask.getRebuildState());

      // Verify results - only one thread should have successfully executed the rebuild
      assertEquals(1, clearTableCallCount.get(), 
          "clearNSSummaryTable should only be called once due to unified control");
      assertEquals(1, successCount.get(), 
          "Exactly one rebuild should succeed");
      assertEquals(threadCount - 1, rejectedCount.get(), 
          "All other rebuilds should be rejected");
      assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
          "Final state should be IDLE");

    } finally {
      finishLatch.countDown(); // Ensure cleanup
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /**
   * Test ReconUtils getNSSummaryRebuildState integration.
   */
  @Test
  void testReconUtilsIntegration() throws Exception {
    // Test initial state access via ReconUtils
    assertEquals(RebuildState.IDLE, ReconUtils.getNSSummaryRebuildState(),
        "Initial state should be IDLE via ReconUtils");

    // Start a rebuild to test RUNNING state
    CountDownLatch rebuildStarted = new CountDownLatch(1);
    CountDownLatch rebuildCanFinish = new CountDownLatch(1);

    // Setup rebuild to block so we can test state
    doAnswer(invocation -> {
      rebuildStarted.countDown();
      boolean awaitSuccess = rebuildCanFinish.await(5, TimeUnit.SECONDS);
      if (!awaitSuccess) {
        LOG.warn("rebuildCanFinish.await() timed out");
      }
      return null;
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    // Start rebuild in background to test state transitions
    CompletableFuture<TaskResult> rebuild = CompletableFuture.supplyAsync(() -> 
        nsSummaryTask.reprocess(mockOMMetadataManager));

    // Wait for rebuild to start and verify RUNNING state
    assertTrue(rebuildStarted.await(5, TimeUnit.SECONDS), 
        "Rebuild should start within timeout");
    assertEquals(RebuildState.RUNNING, ReconUtils.getNSSummaryRebuildState(),
        "State should be RUNNING during rebuild");

    // Complete rebuild and verify IDLE state
    rebuildCanFinish.countDown();
    TaskResult result = rebuild.get(5, TimeUnit.SECONDS);
    assertTrue(result.isTaskSuccess(), "Rebuild should succeed");
    assertEquals(RebuildState.IDLE, ReconUtils.getNSSummaryRebuildState(),
        "State should return to IDLE after completion");
  }

  /**
   * Test state transitions during exception scenarios.
   */
  @Test
  void testStateTransitionsDuringExceptions() throws Exception {
    // Test exception during clearNSSummaryTable
    doThrow(new RuntimeException("Unexpected error"))
        .when(mockNamespaceSummaryManager).clearNSSummaryTable();

    TaskResult result = nsSummaryTask.reprocess(mockOMMetadataManager);
    
    assertFalse(result.isTaskSuccess(), "Rebuild should fail on exception");
    assertEquals(RebuildState.FAILED, NSSummaryTask.getRebuildState(),
        "State should be FAILED after exception");

    // Verify we can recover from FAILED state
    // Setup successful rebuild by default - no exception thrown
    doNothing().when(mockNamespaceSummaryManager).clearNSSummaryTable();
    
    TaskResult recoveryResult = nsSummaryTask.reprocess(mockOMMetadataManager);
    assertTrue(recoveryResult.isTaskSuccess(), "Recovery rebuild should succeed");
    assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
        "State should be IDLE after recovery");
  }

  /**
   * Test that interrupted threads are handled properly.
   */
  @Test
  void testInterruptedThreadHandling() throws Exception {
    CountDownLatch rebuildStarted = new CountDownLatch(1);
    AtomicBoolean wasInterrupted = new AtomicBoolean(false);

    // Setup rebuild to detect interruption
    doAnswer(invocation -> {
      rebuildStarted.countDown();
      try {
        Thread.sleep(5000); // Long sleep to ensure interruption
      } catch (InterruptedException e) {
        wasInterrupted.set(true);
        throw new RuntimeException("Interrupted", e);
      }
      return null;
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    // Start rebuild in separate thread
    Thread rebuildThread = new Thread(() -> {
      nsSummaryTask.reprocess(mockOMMetadataManager);
    });
    rebuildThread.start();

    // Wait for rebuild to start
    assertTrue(rebuildStarted.await(5, TimeUnit.SECONDS),
        "Rebuild should start");
    assertEquals(RebuildState.RUNNING, NSSummaryTask.getRebuildState(),
        "State should be RUNNING");

    // Interrupt the thread
    rebuildThread.interrupt();
    rebuildThread.join(5000);

    // Verify interruption was handled
    assertTrue(wasInterrupted.get(), "Thread should have been interrupted");
    assertEquals(RebuildState.FAILED, NSSummaryTask.getRebuildState(),
        "State should be FAILED after interruption");
  }
}
