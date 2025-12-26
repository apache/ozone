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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
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
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconFileMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTask.RebuildState;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unified and controlled sync access to
 * retrigger of build of NSSummary tree using queue-based architecture.
 *
 * <p>These tests verify that the queue-based unified control mechanism
 * correctly handles concurrent queueReInitializationEvent() calls in production.
 *
 * <p>Execution Flow:
 * <pre>
 * Multiple Concurrent Callers
 *         ↓  ↓  ↓
 * queueReInitializationEvent()  [Thread-safe public API]
 *         ↓
 * BlockingQueue&lt;ReconEvent&gt;     [Serialization layer]
 *         ↓
 * Single Async Thread           [Sequential processing]
 *         ↓
 * processReInitializationEvent()
 *         ↓
 * reInitializeTasks()
 *         ↓
 * task.reprocess()              [Only ONE execution at a time]
 * </pre>
 */
public class TestNSSummaryUnifiedControl {

  private static final Logger LOG = LoggerFactory.getLogger(TestNSSummaryUnifiedControl.class);

  private ReconTaskControllerImpl taskController;
  private ReconNamespaceSummaryManager mockNamespaceSummaryManager;
  private ReconOMMetadataManager mockReconOMMetadataManager;
  private OzoneConfiguration ozoneConfiguration;

  @BeforeEach
  void setUp() throws Exception {
    // Reset static state before each test
    NSSummaryTask.resetRebuildState();

    // Create mocks
    mockNamespaceSummaryManager = mock(ReconNamespaceSummaryManager.class);
    mockReconOMMetadataManager = mock(ReconOMMetadataManager.class);
    ozoneConfiguration = new OzoneConfiguration();

    // Configure small buffer for easier testing
    ozoneConfiguration.setInt(ReconServerConfigKeys.OZONE_RECON_OM_EVENT_BUFFER_CAPACITY, 100);

    // Setup task controller
    ReconTaskStatusUpdaterManager mockTaskStatusUpdaterManager = mock(ReconTaskStatusUpdaterManager.class);
    ReconTaskStatusUpdater mockTaskStatusUpdater = mock(ReconTaskStatusUpdater.class);
    when(mockTaskStatusUpdaterManager.getTaskStatusUpdater(any())).thenReturn(mockTaskStatusUpdater);

    ReconDBProvider reconDbProvider = mock(ReconDBProvider.class);
    when(reconDbProvider.getDbStore()).thenReturn(mock(DBStore.class));
    when(reconDbProvider.getStagedReconDBProvider()).thenReturn(reconDbProvider);

    ReconContainerMetadataManager reconContainerMgr = mock(ReconContainerMetadataManager.class);
    ReconGlobalStatsManager reconGlobalStatsManager = mock(ReconGlobalStatsManager.class);
    ReconFileMetadataManager reconFileMetadataManager = mock(ReconFileMetadataManager.class);

    taskController = new ReconTaskControllerImpl(ozoneConfiguration, new HashSet<>(),
        mockTaskStatusUpdaterManager, reconDbProvider, reconContainerMgr, mockNamespaceSummaryManager,
        reconGlobalStatsManager, reconFileMetadataManager);

    // Register testable NSSummaryTask instance
    taskController.registerTask(createTestableNSSummaryTask());

    // Setup mock OM metadata manager with checkpoint support
    setupMockOMMetadataManager();
    taskController.updateOMMetadataManager(mockReconOMMetadataManager);

    // Setup successful rebuild by default
    doNothing().when(mockNamespaceSummaryManager).clearNSSummaryTable();

    // Start async processing
    taskController.start();
  }

  @AfterEach
  void tearDown() {
    // Reset static state after each test
    NSSummaryTask.resetRebuildState();

    // Shutdown task controller
    if (taskController != null) {
      taskController.stop();
    }
  }

  private void setupMockOMMetadataManager() throws IOException {
    DBStore mockDBStore = mock(DBStore.class);
    File mockDbLocation = mock(File.class);
    DBCheckpoint mockCheckpoint = mock(DBCheckpoint.class);
    Path mockCheckpointPath = Paths.get("/tmp/test/checkpoint");

    when(mockReconOMMetadataManager.getStore()).thenReturn(mockDBStore);
    when(mockDBStore.getDbLocation()).thenReturn(mockDbLocation);
    when(mockDbLocation.getParent()).thenReturn("/tmp/test");
    when(mockDBStore.getCheckpoint(anyString(), any(Boolean.class))).thenReturn(mockCheckpoint);
    when(mockCheckpoint.getCheckpointLocation()).thenReturn(mockCheckpointPath);

    ReconOMMetadataManager mockCheckpointedManager = mock(ReconOMMetadataManager.class);
    when(mockCheckpointedManager.getStore()).thenReturn(mockDBStore);
    when(mockReconOMMetadataManager.createCheckpointReconMetadataManager(any(), any()))
        .thenReturn(mockCheckpointedManager);
  }

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
        Collection<Callable<Boolean>> tasks = new ArrayList<>();

        try {
          getReconNamespaceSummaryManager().clearNSSummaryTable();
        } catch (IOException ioEx) {
          LOG.error("Unable to clear NSSummary table in Recon DB. ", ioEx);
          NSSummaryTask.setRebuildStateToFailed();
          return buildTaskResult(false);
        }

        tasks.add(() -> true);
        tasks.add(() -> true);
        tasks.add(() -> true);

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

          if (success) {
            NSSummaryTask.resetRebuildState();
            LOG.info("Test NSSummary tree reprocess completed successfully.");
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
   * Test single successful rebuild via queue.
   */
  @Test
  void testSingleSuccessfulRebuild() throws Exception {
    AtomicBoolean rebuildExecuted = new AtomicBoolean(false);
    CountDownLatch rebuildLatch = new CountDownLatch(1);

    doAnswer(invocation -> {
      rebuildExecuted.set(true);
      rebuildLatch.countDown();
      return null;
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    // Queue rebuild via production API
    ReconTaskController.ReInitializationResult result =
        taskController.queueReInitializationEvent(
            ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);

    assertEquals(ReconTaskController.ReInitializationResult.SUCCESS, result,
        "Rebuild should be queued successfully");

    // Wait for async processing
    assertTrue(rebuildLatch.await(10, TimeUnit.SECONDS),
        "Rebuild should execute");
    assertTrue(rebuildExecuted.get(), "Rebuild should have executed");

    // wait for 5 secs for state to return to IDLE
    GenericTestUtils.waitFor(() -> NSSummaryTask.getRebuildState() == RebuildState.IDLE,
        100, 5000);
    assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
        "State should return to IDLE after successful rebuild");
  }

  /**
   * Test rebuild failure sets proper state.
   */
  @Test
  void testRebuildFailure() throws Exception {
    CountDownLatch failureLatch = new CountDownLatch(1);

    doAnswer(invocation -> {
      failureLatch.countDown();
      throw new IOException("Test failure");
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    ReconTaskController.ReInitializationResult result =
        taskController.queueReInitializationEvent(
            ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);

    assertEquals(ReconTaskController.ReInitializationResult.SUCCESS, result,
        "Rebuild should be queued successfully");

    assertTrue(failureLatch.await(10, TimeUnit.SECONDS),
        "Rebuild should be attempted");

    // Wait for 5 secs time for state update
    GenericTestUtils.waitFor(() -> NSSummaryTask.getRebuildState() == RebuildState.FAILED,
        100, 5000);
    assertEquals(RebuildState.FAILED, NSSummaryTask.getRebuildState(),
        "State should be FAILED after rebuild failure");
  }

  /**
   * Test rebuild can be triggered again after failure.
   */
  @Test
  void testRebuildAfterFailure() throws Exception {
    CountDownLatch firstAttempt = new CountDownLatch(1);
    CountDownLatch secondAttempt = new CountDownLatch(1);
    AtomicInteger attemptCount = new AtomicInteger(0);

    // Setup mock to fail first time, succeed second time
    doAnswer(invocation -> {
      int attempt = attemptCount.incrementAndGet();
      LOG.info("clearNSSummaryTable attempt #{}", attempt);
      if (attempt == 1) {
        firstAttempt.countDown();
        throw new IOException("First failure");
      } else {
        secondAttempt.countDown();
        return null;
      }
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    // First rebuild fails
    ReconTaskController.ReInitializationResult result1 =
        taskController.queueReInitializationEvent(
            ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);
    assertEquals(ReconTaskController.ReInitializationResult.SUCCESS, result1,
        "First event should be queued successfully");

    assertTrue(firstAttempt.await(10, TimeUnit.SECONDS), "First rebuild should be attempted");
    GenericTestUtils.waitFor(() -> NSSummaryTask.getRebuildState() == RebuildState.FAILED,
        100, 5000);
    assertEquals(RebuildState.FAILED, NSSummaryTask.getRebuildState(),
        "State should be FAILED after first rebuild");

    // Second rebuild succeeds - must wait for time-based retry delay to expire
    // This is a deliberate time-based test of the retry mechanism
    int retryDelayMs = 2100; // RETRY_DELAY_MS (2000ms) + buffer
    long deadline = System.currentTimeMillis() + retryDelayMs;
    GenericTestUtils.waitFor(() -> System.currentTimeMillis() >= deadline,
        100, retryDelayMs + 1000);

    ReconTaskController.ReInitializationResult result2 =
        taskController.queueReInitializationEvent(
            ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);
    assertEquals(ReconTaskController.ReInitializationResult.SUCCESS, result2,
        "Second event should be queued successfully after retry delay");

    assertTrue(secondAttempt.await(10, TimeUnit.SECONDS), "Second rebuild should be attempted");
    GenericTestUtils.waitFor(() -> NSSummaryTask.getRebuildState() == RebuildState.IDLE,
        100, 5000);
    assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
        "State should be IDLE after successful rebuild");
  }

  /**
   * Test multiple concurrent queueReInitializationEvent() calls.
   * <p>
   * This is the KEY test for production behavior - multiple threads
   * simultaneously calling queueReInitializationEvent(), which is what
   * actually happens in production (not direct reprocess() calls).
   *
   * <p>Important: The queue-based architecture provides SEQUENTIAL processing,
   * not event deduplication. Multiple successfully queued events will execute
   * sequentially (not concurrently). The AtomicReference in NSSummaryTask
   * prevents concurrent execution within a single reprocess() call.
   */
  @Test
  void testMultipleConcurrentAttempts() throws Exception {
    ConcurrentTestContext ctx = new ConcurrentTestContext(5);

    NSSummaryTask.resetRebuildState();
    assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(), "Initial state must be IDLE");

    setupConcurrentExecutionTracking(ctx);

    ExecutorService executor = Executors.newFixedThreadPool(ctx.getThreadCount());
    List<CompletableFuture<ReconTaskController.ReInitializationResult>> futures =
        launchConcurrentQueueRequests(ctx, executor);

    try {
      coordinateConcurrentExecution(ctx, futures);
      verifyConcurrentExecutionResults(ctx, futures);
    } finally {
      ctx.getFirstRebuildCanComplete().countDown();
      executor.shutdown();
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    }
  }

  private void setupConcurrentExecutionTracking(ConcurrentTestContext ctx) throws IOException {
    doAnswer(invocation -> {
      int callNum = ctx.getClearTableCallCount().incrementAndGet();
      int currentConcurrent = ctx.getConcurrentExecutions().incrementAndGet();

      ctx.getMaxConcurrentExecutions().updateAndGet(max -> Math.max(max, currentConcurrent));
      LOG.info("clearNSSummaryTable call #{}, concurrent executions: {}, state: {}",
          callNum, currentConcurrent, NSSummaryTask.getRebuildState());

      try {
        if (callNum == 1) {
          ctx.getFirstRebuildStarted().countDown();
          boolean awaitSuccess = ctx.getFirstRebuildCanComplete().await(15, TimeUnit.SECONDS);
          if (!awaitSuccess) {
            LOG.error("firstRebuildCanComplete.await() timed out");
          }
        } else {
          Thread.sleep(100); // Simulate operation time (acceptable use in mock)
        }
      } finally {
        ctx.getConcurrentExecutions().decrementAndGet();
      }
      return null;
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();
  }

  private List<CompletableFuture<ReconTaskController.ReInitializationResult>>
      launchConcurrentQueueRequests(ConcurrentTestContext ctx, ExecutorService executor) {
    List<CompletableFuture<ReconTaskController.ReInitializationResult>> futures = new ArrayList<>();

    for (int i = 0; i < ctx.threadCount; i++) {
      final int threadId = i;
      futures.add(CompletableFuture.supplyAsync(() ->
          executeQueueRequest(ctx, threadId), executor));
    }
    return futures;
  }

  private ReconTaskController.ReInitializationResult executeQueueRequest(
      ConcurrentTestContext ctx, int threadId) {
    try {
      ctx.getAllThreadsReady().countDown();
      LOG.info("Thread {} ready, waiting for all threads", threadId);

      if (!ctx.getAllThreadsReady().await(10, TimeUnit.SECONDS)) {
        throw new RuntimeException("Not all threads ready in time");
      }

      Thread.sleep(threadId * 10L); // Staggered delay for race conditions

      LOG.info("Thread {} calling queueReInitializationEvent()", threadId);
      ctx.getTotalQueueAttempts().incrementAndGet();

      ReconTaskController.ReInitializationResult result =
          taskController.queueReInitializationEvent(
              ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);

      if (result == ReconTaskController.ReInitializationResult.SUCCESS) {
        ctx.getSuccessfulQueueCount().incrementAndGet();
      }

      LOG.info("Thread {} completed with result={}", threadId, result);
      return result;
    } catch (InterruptedException e) {
      LOG.error("Thread {} interrupted", threadId, e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private void coordinateConcurrentExecution(ConcurrentTestContext ctx,
                                             List<CompletableFuture<ReconTaskController.ReInitializationResult>>
                                                 futures)
      throws Exception {
    assertTrue(ctx.getFirstRebuildStarted().await(15, TimeUnit.SECONDS), "First rebuild should start");
    LOG.info("First rebuild started, state: {}", NSSummaryTask.getRebuildState());

    GenericTestUtils.waitFor(() -> ctx.getTotalQueueAttempts().get() >= ctx.getThreadCount() / 2,
        100, 5000);

    ctx.getFirstRebuildCanComplete().countDown();

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(20, TimeUnit.SECONDS);

    // Wait for all queued events to be processed and state to return to IDLE
    // Use longer timeout and smaller check interval for more robust waiting
    GenericTestUtils.waitFor(() -> {
      RebuildState state = NSSummaryTask.getRebuildState();
      LOG.info("Current state: {}, clearTableCallCount: {}", state, ctx.getClearTableCallCount().get());
      return state == RebuildState.IDLE;
    }, 100, 20000);
  }

  private void verifyConcurrentExecutionResults(ConcurrentTestContext ctx,
                                                List<CompletableFuture<ReconTaskController.ReInitializationResult>>
                                                    futures)
      throws Exception {
    ResultCounts counts = collectResultCounts(futures);

    LOG.info("Test completed - Total queue attempts: {}, Successful queues: {}, " +
            "Result breakdown: SUCCESS={}, RETRY_LATER={}, MAX_RETRIES={}, " +
            "ClearTable calls: {}, Max concurrent: {}, Final state: {}",
        ctx.getTotalQueueAttempts().get(), ctx.getSuccessfulQueueCount().get(),
        counts.getSuccessCount(), counts.getRetryLaterCount(), counts.getMaxRetriesCount(),
        ctx.getClearTableCallCount().get(), ctx.getMaxConcurrentExecutions().get(),
        NSSummaryTask.getRebuildState());

    assertEquals(1, ctx.getMaxConcurrentExecutions().get(),
        "Should never have concurrent executions - queue provides serialization");
    assertEquals(ctx.getThreadCount(), ctx.getTotalQueueAttempts().get(),
        "All threads should have attempted to queue events");
    assertTrue(ctx.getSuccessfulQueueCount().get() >= 1,
        "At least one thread should have successfully queued rebuild");
    assertThat(ctx.getClearTableCallCount().get())
        .as("At least one rebuild should execute and not exceed successfully queued events")
        .isPositive()
        .isLessThanOrEqualTo(ctx.getSuccessfulQueueCount().get());

    // Final verification that state is IDLE - wait one more time to ensure stability
    GenericTestUtils.waitFor(() -> NSSummaryTask.getRebuildState() == RebuildState.IDLE,
        100, 5000);
    assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
        "Final state should be IDLE after all rebuilds complete");

    LOG.info("VERIFIED: Queue architecture prevents concurrent executions. " +
        "Multiple events can be queued but execute sequentially.");
  }

  private ResultCounts collectResultCounts(
      List<CompletableFuture<ReconTaskController.ReInitializationResult>> futures)
      throws Exception {
    ResultCounts counts = new ResultCounts();
    for (CompletableFuture<ReconTaskController.ReInitializationResult> future : futures) {
      ReconTaskController.ReInitializationResult result = future.get();
      switch (result) {
      case SUCCESS:
        counts.successCount++;
        break;
      case RETRY_LATER:
        counts.retryLaterCount++;
        break;
      case MAX_RETRIES_EXCEEDED:
        counts.maxRetriesCount++;
        break;
      default:
        LOG.warn("Unexpected result: {}", result);
      }
    }
    return counts;
  }

  /**
   * Context for concurrent test execution tracking.
   */
  private static class ConcurrentTestContext {
    private final int threadCount;
    private final CountDownLatch allThreadsReady;
    private final CountDownLatch firstRebuildStarted;
    private final CountDownLatch firstRebuildCanComplete;
    private final AtomicInteger clearTableCallCount;
    private final AtomicInteger concurrentExecutions;
    private final AtomicInteger maxConcurrentExecutions;
    private final AtomicInteger successfulQueueCount;
    private final AtomicInteger totalQueueAttempts;

    ConcurrentTestContext(int threadCount) {
      this.threadCount = threadCount;
      this.allThreadsReady = new CountDownLatch(threadCount);
      this.firstRebuildStarted = new CountDownLatch(1);
      this.firstRebuildCanComplete = new CountDownLatch(1);
      this.clearTableCallCount = new AtomicInteger(0);
      this.concurrentExecutions = new AtomicInteger(0);
      this.maxConcurrentExecutions = new AtomicInteger(0);
      this.successfulQueueCount = new AtomicInteger(0);
      this.totalQueueAttempts = new AtomicInteger(0);
    }

    public int getThreadCount() {
      return threadCount;
    }

    public CountDownLatch getAllThreadsReady() {
      return allThreadsReady;
    }

    public CountDownLatch getFirstRebuildStarted() {
      return firstRebuildStarted;
    }

    public CountDownLatch getFirstRebuildCanComplete() {
      return firstRebuildCanComplete;
    }

    public AtomicInteger getClearTableCallCount() {
      return clearTableCallCount;
    }

    public AtomicInteger getConcurrentExecutions() {
      return concurrentExecutions;
    }

    public AtomicInteger getMaxConcurrentExecutions() {
      return maxConcurrentExecutions;
    }

    public AtomicInteger getSuccessfulQueueCount() {
      return successfulQueueCount;
    }

    public AtomicInteger getTotalQueueAttempts() {
      return totalQueueAttempts;
    }
  }

  /**
   * Result counts from concurrent execution.
   */
  private static class ResultCounts {
    private long successCount = 0;
    private long retryLaterCount = 0;
    private long maxRetriesCount = 0;

    public long getSuccessCount() {
      return successCount;
    }

    public long getRetryLaterCount() {
      return retryLaterCount;
    }

    public long getMaxRetriesCount() {
      return maxRetriesCount;
    }
  }

  /**
   * Test ReconUtils getNSSummaryRebuildState integration.
   */
  @Test
  void testReconUtilsIntegration() throws Exception {
    assertEquals(RebuildState.IDLE, ReconUtils.getNSSummaryRebuildState(),
        "Initial state should be IDLE via ReconUtils");

    CountDownLatch rebuildStarted = new CountDownLatch(1);
    CountDownLatch rebuildCanFinish = new CountDownLatch(1);

    doAnswer(invocation -> {
      rebuildStarted.countDown();
      boolean awaitSuccess = rebuildCanFinish.await(10, TimeUnit.SECONDS);
      if (!awaitSuccess) {
        LOG.warn("rebuildCanFinish.await() timed out");
      }
      return null;
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    taskController.queueReInitializationEvent(
        ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);

    assertTrue(rebuildStarted.await(10, TimeUnit.SECONDS),
        "Rebuild should start");
    assertEquals(RebuildState.RUNNING, ReconUtils.getNSSummaryRebuildState(),
        "State should be RUNNING during rebuild");

    rebuildCanFinish.countDown();
    GenericTestUtils.waitFor(() -> ReconUtils.getNSSummaryRebuildState() == RebuildState.IDLE,
        100, 5000);
    assertEquals(RebuildState.IDLE, ReconUtils.getNSSummaryRebuildState(),
        "State should return to IDLE after completion");
  }

  /**
   * Test state transitions during exception scenarios.
   */
  @Test
  void testStateTransitionsDuringExceptions() throws Exception {
    CountDownLatch exceptionLatch = new CountDownLatch(1);
    CountDownLatch recoveryLatch = new CountDownLatch(1);
    AtomicInteger callCount = new AtomicInteger(0);

    // Setup mock to throw exception first time, succeed second time
    doAnswer(invocation -> {
      int call = callCount.incrementAndGet();
      LOG.info("clearNSSummaryTable call #{}", call);
      if (call == 1) {
        exceptionLatch.countDown();
        throw new RuntimeException("Unexpected error");
      } else {
        recoveryLatch.countDown();
        return null;
      }
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    // First rebuild throws exception
    ReconTaskController.ReInitializationResult result1 =
        taskController.queueReInitializationEvent(
            ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);
    assertEquals(ReconTaskController.ReInitializationResult.SUCCESS, result1,
        "First event should be queued successfully");

    assertTrue(exceptionLatch.await(10, TimeUnit.SECONDS),
        "Exception should occur");
    GenericTestUtils.waitFor(() -> NSSummaryTask.getRebuildState() == RebuildState.FAILED,
        100, 5000);
    assertEquals(RebuildState.FAILED, NSSummaryTask.getRebuildState(),
        "State should be FAILED after exception");

    // Second rebuild succeeds (recovery) - must wait for time-based retry delay to expire
    // This is a deliberate time-based test of the retry mechanism
    int retryDelayMs = 2100; // RETRY_DELAY_MS (2000ms) + buffer
    long deadline = System.currentTimeMillis() + retryDelayMs;
    GenericTestUtils.waitFor(() -> System.currentTimeMillis() >= deadline,
        100, retryDelayMs + 1000);

    ReconTaskController.ReInitializationResult result2 =
        taskController.queueReInitializationEvent(
            ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);
    assertEquals(ReconTaskController.ReInitializationResult.SUCCESS, result2,
        "Second event should be queued successfully after retry delay");

    assertTrue(recoveryLatch.await(10, TimeUnit.SECONDS), "Recovery should execute");
    GenericTestUtils.waitFor(() -> NSSummaryTask.getRebuildState() == RebuildState.IDLE,
        100, 5000);
    assertEquals(RebuildState.IDLE, NSSummaryTask.getRebuildState(),
        "State should be IDLE after recovery");
  }

  /**
   * Test checkpoint creation failure and retry mechanism.
   */
  @Test
  void testCheckpointCreationFailureRetry() throws Exception {
    ReconTaskControllerImpl controllerSpy = spy(taskController);
    doThrow(new IOException("Checkpoint creation failed"))
        .when(controllerSpy).createOMCheckpoint(any());

    // First few attempts should return RETRY_LATER
    ReconTaskController.ReInitializationResult result1 =
        controllerSpy.queueReInitializationEvent(
            ReconTaskReInitializationEvent.ReInitializationReason.BUFFER_OVERFLOW);
    assertEquals(ReconTaskController.ReInitializationResult.RETRY_LATER, result1,
        "First attempt should return RETRY_LATER due to checkpoint failure");

    // After delay, try again - must wait for time-based retry delay to expire
    int retryDelayMs = 2100; // RETRY_DELAY_MS (2000ms) + buffer
    long deadline = System.currentTimeMillis() + retryDelayMs;
    GenericTestUtils.waitFor(() -> System.currentTimeMillis() >= deadline,
        100, retryDelayMs + 1000);
    ReconTaskController.ReInitializationResult result2 =
        controllerSpy.queueReInitializationEvent(
            ReconTaskReInitializationEvent.ReInitializationReason.BUFFER_OVERFLOW);
    assertEquals(ReconTaskController.ReInitializationResult.RETRY_LATER, result2,
        "Second attempt should also return RETRY_LATER");

    verify(controllerSpy, times(2)).createOMCheckpoint(any());
  }

  /**
   * Test event buffer integration with concurrent queueing.
   */
  @Test
  void testEventBufferWithConcurrentQueueing() throws Exception {
    int initialBufferSize = taskController.getEventBufferSize();
    LOG.info("Initial buffer size: {}", initialBufferSize);

    CountDownLatch queuedLatch = new CountDownLatch(1);
    doAnswer(invocation -> {
      queuedLatch.countDown();
      Thread.sleep(100);
      return null;
    }).when(mockNamespaceSummaryManager).clearNSSummaryTable();

    // Queue an event
    ReconTaskController.ReInitializationResult result =
        taskController.queueReInitializationEvent(
            ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);

    assertEquals(ReconTaskController.ReInitializationResult.SUCCESS, result,
        "Event should be successfully queued");

    // Event should be in buffer or being processed
    assertTrue(queuedLatch.await(10, TimeUnit.SECONDS),
        "Event should be processed");

    LOG.info("Event buffer integration test completed successfully");
  }
}
