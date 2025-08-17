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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
        
        String threadName = Thread.currentThread().getName();
        LOG.info("TEST executeReprocess called by thread: {}, using manager: {}", 
            threadName, getReconNamespaceSummaryManager().getClass().getSimpleName());
        
        // Initialize a list of tasks to run in parallel (empty for testing)
        Collection<Callable<Boolean>> tasks = new ArrayList<>();

        try {
          // This will call the mocked clearNSSummaryTable (might throw Exception for failure tests)
          LOG.info("TEST: About to call clearNSSummaryTable on: {}", getReconNamespaceSummaryManager());
          getReconNamespaceSummaryManager().clearNSSummaryTable();
          LOG.info("TEST: clearNSSummaryTable call completed");
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
          
          // Don't reset state here - let the main reprocess method handle state management
          // The test should not interfere with state transitions during execution
          if (success) {
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
  void testMultipleConcurrentAttempts() throws Exception {
    int threadCount = 5;
    CountDownLatch allThreadsReady = new CountDownLatch(threadCount);
    CountDownLatch startSimultaneously = new CountDownLatch(1);
    CountDownLatch allThreadsStartedReprocess = new CountDownLatch(threadCount);
    CountDownLatch finishLatch = new CountDownLatch(1);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger rejectedCount = new AtomicInteger(0);
    AtomicInteger clearTableCallCount = new AtomicInteger(0);
    
    // Track which threads are part of our test to filter out external calls
    final Set<String> testThreadNames = Collections.synchronizedSet(new HashSet<>());

    // Ensure clean initial state
    NSSummaryTask.resetRebuildState();
    assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(), 
        "Initial state must be IDLE");

    // Setup rebuild to block and count calls with additional debugging
    doAnswer(invocation -> {
      String threadName = Thread.currentThread().getName();
      RebuildState currentState = NSSummaryTask.getRebuildState();
      
      // Count all calls but distinguish between test threads and external threads
      boolean isTestThread = threadName.contains("ForkJoinPool") || testThreadNames.contains(threadName);
      if (isTestThread) {
        LOG.error("Test Thread: {} - Clearing NSSummary table in Recon DB.", Thread.currentThread().getName());
        int callNum = clearTableCallCount.incrementAndGet();
        LOG.info("clearNSSummaryTable called #{} by test thread: {}, current state: {}", 
            callNum, threadName, currentState);
        
        // Wait for ALL threads to have attempted to call reprocess() before allowing any to proceed
        // This ensures we test true simultaneous access to the compareAndSet operations
        try {
          boolean awaitSuccess = allThreadsStartedReprocess.await(10, TimeUnit.SECONDS);
          if (!awaitSuccess) {
            LOG.warn("allThreadsStartedReprocess.await() timed out");
          }
          // Then wait for test to signal completion
          awaitSuccess = finishLatch.await(10, TimeUnit.SECONDS);
          if (!awaitSuccess) {
            LOG.warn("finishLatch.await() timed out");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.warn("Thread interrupted while waiting");
        }
        
        if (callNum > 1) {
          // If we get a second call from our test threads during the SAME test execution,
          // this indicates the unified control failed
          LOG.error("UNEXPECTED: clearNSSummaryTable called multiple times (call #{}) by test thread: {}, state: {}", 
              callNum, threadName, currentState);
        }
      } else {
        LOG.error("External Thread: {} - Clearing NSSummary table in Recon DB.", Thread.currentThread().getName());
        // Log external calls that shouldn't be counted but might indicate CI interference
        LOG.warn("clearNSSummaryTable called by EXTERNAL thread: {}, state: {} - this may be CI interference",
            threadName, currentState);
      }
      return null;
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CompletableFuture<Void>[] futures = new CompletableFuture[threadCount];

    try {
      // Launch multiple concurrent rebuilds with proper synchronization
      for (int i = 0; i < threadCount; i++) {
        final int threadId = i;
        futures[i] = CompletableFuture.runAsync(() -> {
          try {
            // Register this thread as part of our test
            String threadName = Thread.currentThread().getName();
            testThreadNames.add(threadName);
            
            // Signal this thread is ready
            allThreadsReady.countDown();
            // Wait for all threads to be ready before starting simultaneously
            allThreadsReady.await(5, TimeUnit.SECONDS);
            
            // All threads now wait for the signal to start simultaneously
            startSimultaneously.await(5, TimeUnit.SECONDS);

            LOG.info("Thread {} ({}) attempting rebuild, current state: {}", threadId, threadName,
                NSSummaryTask.getRebuildState());
            
            // Signal that this thread has started calling reprocess()
            allThreadsStartedReprocess.countDown();
            
            TaskResult result = nsSummaryTask.reprocess(mockOMMetadataManager);
            if (result.isTaskSuccess()) {
              int count = successCount.incrementAndGet();
              LOG.info("Thread {} ({}) rebuild succeeded (success #{})", threadId, threadName, count);
            } else {
              int count = rejectedCount.incrementAndGet();
              LOG.info("Thread {} ({}) rebuild rejected (rejection #{})", threadId, threadName, count);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Thread {} interrupted", threadId);
          }
        }, executor);
      }

      // Wait for all threads to be ready, then start them simultaneously
      assertTrue(allThreadsReady.await(5, TimeUnit.SECONDS), 
          "All threads should be ready");
      
      // Start all threads simultaneously
      startSimultaneously.countDown();
      
      // Wait for all threads to have started their reprocess() calls
      assertTrue(allThreadsStartedReprocess.await(10, TimeUnit.SECONDS), 
          "All threads should start reprocess calls");
      
      // Verify that state is RUNNING (at least one thread acquired the lock)
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
      
      // Additional verification that our mock was actually used
      verify(mockNamespaceSummaryManager, times(1)).clearNSSummaryTable();

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
   * Test that ReconUtils async trigger respects unified control.
   */
  @Test
  void testReconUtilsRespectsUnifiedControl() throws Exception {
    CountDownLatch firstRebuildStarted = new CountDownLatch(1);
    CountDownLatch firstRebuildCanFinish = new CountDownLatch(1);

    // Setup first rebuild to block
    doAnswer(invocation -> {
      firstRebuildStarted.countDown();
      boolean awaitSuccess = firstRebuildCanFinish.await(5, TimeUnit.SECONDS);
      if (!awaitSuccess) {
        LOG.warn("firstRebuildCanFinish.await() timed out");
      }
      return null;
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    // Start first rebuild via NSSummaryTask directly
    CompletableFuture<TaskResult> directRebuild = CompletableFuture.supplyAsync(() -> 
        nsSummaryTask.reprocess(mockOMMetadataManager));

    // Wait for first rebuild to start
    assertTrue(firstRebuildStarted.await(5, TimeUnit.SECONDS),
        "First rebuild should start");
    assertEquals(RebuildState.RUNNING, NSSummaryTask.getRebuildState(),
        "State should be RUNNING");

    // Try to trigger via ReconUtils - should still respect the running state
    // (The async execution will be queued but the actual rebuild will be rejected)
    boolean triggered = ReconUtils.triggerAsyncNSSummaryRebuild(
        mockNamespaceSummaryManager, mockReconOMMetadataManager);
    assertTrue(triggered, "ReconUtils should queue the async request");

    // State should still be RUNNING from the first rebuild
    assertEquals(RebuildState.RUNNING, NSSummaryTask.getRebuildState(),
        "State should still be RUNNING from first rebuild");

    // Complete first rebuild
    firstRebuildCanFinish.countDown();
    TaskResult result = directRebuild.get(5, TimeUnit.SECONDS);
    assertTrue(result.isTaskSuccess(), "First rebuild should succeed");

    // Final state should be IDLE
    assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
        "Final state should be IDLE");
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
