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

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_EVENT_BUFFER_CAPACITY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_EVENT_BUFFER_CAPACITY_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_THREAD_COUNT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_THREAD_COUNT_KEY;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.metrics.ReconTaskControllerMetrics;
import org.apache.hadoop.ozone.recon.metrics.ReconTaskMetrics;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconFileMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconGlobalStatsManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.apache.hadoop.ozone.recon.tasks.types.NamedCallableTask;
import org.apache.hadoop.ozone.recon.tasks.types.TaskExecutionException;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of ReconTaskController.
 */
public class ReconTaskControllerImpl implements ReconTaskController {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconTaskControllerImpl.class);
  private static final String REPROCESS_STAGING = "REPROCESS_STAGING";
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 30L;
  private final ReconDBProvider reconDBProvider;
  private final ReconContainerMetadataManager reconContainerMetadataManager;
  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final ReconGlobalStatsManager reconGlobalStatsManager;
  private final ReconFileMetadataManager reconFileMetadataManager;

  private Map<String, ReconOmTask> reconOmTasks;
  private ExecutorService executorService;
  private final int threadCount;
  private final ReconTaskStatusUpdaterManager taskStatusUpdaterManager;
  private final OMUpdateEventBuffer eventBuffer;
  private ExecutorService eventProcessingExecutor;
  private final AtomicBoolean tasksFailed = new AtomicBoolean(false);
  private volatile ReconOMMetadataManager currentOMMetadataManager;
  private final OzoneConfiguration configuration;

  // Metrics
  private final ReconTaskControllerMetrics controllerMetrics;
  private final ReconTaskMetrics taskMetrics;

  // Retry logic for event processing failures
  private AtomicInteger eventProcessRetryCount = new AtomicInteger(0);
  private AtomicLong lastRetryTimestamp = new AtomicLong(0);
  private static final int MAX_EVENT_PROCESS_RETRIES = 6;
  private static final long RETRY_DELAY_MS = 2000; // 2 seconds

  @Inject
  @SuppressWarnings("checkstyle:ParameterNumber")
  public ReconTaskControllerImpl(OzoneConfiguration configuration,
                                 Set<ReconOmTask> tasks,
                                 ReconTaskStatusUpdaterManager taskStatusUpdaterManager,
                                 ReconDBProvider reconDBProvider,
                                 ReconContainerMetadataManager reconContainerMetadataManager,
                                 ReconNamespaceSummaryManager reconNamespaceSummaryManager,
                                 ReconGlobalStatsManager reconGlobalStatsManager,
                                 ReconFileMetadataManager reconFileMetadataManager) {
    this.configuration = configuration;
    this.reconDBProvider = reconDBProvider;
    this.reconContainerMetadataManager = reconContainerMetadataManager;
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.reconGlobalStatsManager = reconGlobalStatsManager;
    this.reconFileMetadataManager = reconFileMetadataManager;
    reconOmTasks = new HashMap<>();
    threadCount = configuration.getInt(OZONE_RECON_TASK_THREAD_COUNT_KEY,
        OZONE_RECON_TASK_THREAD_COUNT_DEFAULT);
    this.taskStatusUpdaterManager = taskStatusUpdaterManager;

    // Initialize metrics
    this.controllerMetrics = ReconTaskControllerMetrics.create();
    this.taskMetrics = ReconTaskMetrics.create();

    int eventBufferCapacity = configuration.getInt(OZONE_RECON_OM_EVENT_BUFFER_CAPACITY,
        OZONE_RECON_OM_EVENT_BUFFER_CAPACITY_DEFAULT);
    this.eventBuffer = new OMUpdateEventBuffer(eventBufferCapacity, controllerMetrics);
    for (ReconOmTask task : tasks) {
      registerTask(task);
    }
  }

  @Override
  public void registerTask(ReconOmTask task) {
    String taskName = task.getTaskName();
    LOG.info("Registered task {} with controller.", taskName);

    // Store task in Task Map.
    reconOmTasks.put(taskName, task);
  }

  /**
   * For every registered task, we try process step twice and then reprocess
   * once (if process failed twice) to absorb the events. If a task has failed
   * reprocess call more than 2 times across events, it is unregistered
   * (ignored).
   * @param events set of events
   */
  @Override
  public synchronized void consumeOMEvents(OMUpdateEventBatch events, OMMetadataManager omMetadataManager) {
    // If tasks have failed, we skip buffering events till we successfully queue reinit event
    if (!events.isEmpty() && !hasTasksFailed()) {
      // Always buffer events for async processing
      boolean buffered = eventBuffer.offer(events);
      if (!buffered) {
        LOG.error("Event buffer is full (capacity: {}). Dropping buffered events and signaling full snapshot. " +
            "Buffer size: {}, Dropped batches: {}", 
            20000, eventBuffer.getQueueSize(), eventBuffer.getDroppedBatches());
        
        // Clear buffer and signal full snapshot requirement
        drainEventBufferAndCleanExistingCheckpoints();
      } else {
        LOG.debug("Buffered event batch with {} events. Buffer queue size: {}", 
            events.getEvents().size(), eventBuffer.getQueueSize());
      }
    }
  }

  /**
   * Reinitializes the registered Recon OM tasks with a new OM Metadata Manager instance.
   *
   * @param omMetadataManager the OM Metadata Manager instance to be used for reinitialization.
   * @param reconOmTaskMap    a map of Recon OM tasks whose lastUpdatedSeqNumber does not match
   *                          the lastUpdatedSeqNumber from the previous run of the 'OmDeltaRequest' task.
   *                          These tasks will be reinitialized to process the delta OM DB updates
   *                          received in the last run of 'OmDeltaRequest'.
   *                          If {@code reconOmTaskMap} is null, all registered Recon OM tasks
   *                          will be reinitialized.
   * @return
   */
  @Override
  public synchronized boolean reInitializeTasks(ReconOMMetadataManager omMetadataManager,
                                                Map<String, ReconOmTask> reconOmTaskMap) {
    LOG.info("Starting Re-initialization of tasks. This is a blocking operation.");
    Collection<NamedCallableTask<ReconOmTask.TaskResult>> tasks = new ArrayList<>();
    Map<String, ReconOmTask> localReconOmTaskMap = reconOmTaskMap;
    if (reconOmTaskMap == null) {
      localReconOmTaskMap = reconOmTasks;
    }
    ReconConstants.resetTableTruncatedFlags();

    ReconDBProvider stagedReconDBProvider;
    try {
      ReconTaskStatusUpdater reprocessTaskStatus = taskStatusUpdaterManager.getTaskStatusUpdater(REPROCESS_STAGING);
      reprocessTaskStatus.recordRunStart();
      stagedReconDBProvider = reconDBProvider.getStagedReconDBProvider();
    } catch (IOException e) {
      LOG.error("Failed to get staged Recon DB provider for reinitialization of tasks.", e);

      // Track checkpoint creation failure
      controllerMetrics.incrReprocessCheckpointFailures();

      recordAllTaskStatus(localReconOmTaskMap, -1, -1);
      return false;
    }

    localReconOmTaskMap.values().forEach(task -> {
      ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(task.getTaskName());
      taskStatusUpdater.recordRunStart();
      tasks.add(new NamedCallableTask<>(task.getTaskName(),
          () -> task.getStagedTask(omMetadataManager, stagedReconDBProvider.getDbStore())
              .reprocess(omMetadataManager)));
    });

    AtomicBoolean isRunSuccessful = new AtomicBoolean(true);
    LOG.info("Submitting {} tasks for parallel reprocessing", tasks.size());
    try {
      CompletableFuture.allOf(tasks.stream()
          .map(task -> {
            // Track reprocess duration per task - start time recorded before async execution
            long reprocessStartTime = Time.monotonicNow();

            return CompletableFuture.supplyAsync(() -> {
              LOG.info("Task {} started execution on thread {}", 
                  task.getTaskName(), Thread.currentThread().getName());
              try {
                ReconOmTask.TaskResult result = task.call();
                LOG.info("Task {} completed execution", task.getTaskName());
                return result;
              } catch (Exception e) {
                // Track reprocess failure per task
                taskMetrics.incrTaskReprocessFailures(task.getTaskName());

                if (e instanceof InterruptedException) {
                  Thread.currentThread().interrupt();
                }
                // Wrap the exception with the task name
                throw new TaskExecutionException(task.getTaskName(), e);
              }
            }, executorService).thenAccept(result -> {
              // Update reprocess duration after task completes (includes queue time)
              long reprocessDuration = Time.monotonicNow() - reprocessStartTime;
              taskMetrics.updateTaskReprocessDuration(task.getTaskName(), reprocessDuration);

              if (!result.isTaskSuccess()) {
                String taskName = result.getTaskName();
                LOG.error("Init failed for task {}.", taskName);

                // Track reprocess failure per task
                taskMetrics.incrTaskReprocessFailures(taskName);

                isRunSuccessful.set(false);
              }
            }).exceptionally(ex -> {
              LOG.error("Task failed with exception: ", ex);
              isRunSuccessful.set(false);
              if (ex.getCause() instanceof TaskExecutionException) {
                TaskExecutionException taskEx = (TaskExecutionException) ex.getCause();
                String taskName = taskEx.getTaskName();
                // Track reprocess failure per task
                taskMetrics.incrTaskReprocessFailures(taskName);
                LOG.error("The above error occurred while trying to execute task: {}", taskName);
              }
              return null;
            });
          }).toArray(CompletableFuture[]::new)).join();
    } catch (CompletionException ce) {
      LOG.error("Completing all tasks failed with exception ", ce);
      isRunSuccessful.set(false);
    } catch (CancellationException ce) {
      LOG.error("Some tasks were cancelled with exception", ce);
      isRunSuccessful.set(false);
    }

    if (isRunSuccessful.get()) {
      try {
        reconDBProvider.replaceStagedDb(stagedReconDBProvider);
        reconNamespaceSummaryManager.reinitialize(reconDBProvider);
        reconContainerMetadataManager.reinitialize(reconDBProvider);
        reconGlobalStatsManager.reinitialize(reconDBProvider);
        reconFileMetadataManager.reinitialize(reconDBProvider);
        recordAllTaskStatus(localReconOmTaskMap, 0, omMetadataManager.getLastSequenceNumberFromDB());

        // Track reprocess success
        controllerMetrics.incrReprocessSuccessCount();

        LOG.info("Re-initialization of tasks completed successfully.");
      } catch (Exception e) {
        LOG.error("Re-initialization of tasks failed.", e);

        // Track stage database failure
        controllerMetrics.incrReprocessStageDatabaseFailures();

        recordAllTaskStatus(localReconOmTaskMap, -1, -1);
        // reinitialize the Recon OM tasks with the original DB provider
        try {
          reconNamespaceSummaryManager.reinitialize(reconDBProvider);
          reconContainerMetadataManager.reinitialize(reconDBProvider);
          reconGlobalStatsManager.reinitialize(reconDBProvider);
          reconFileMetadataManager.reinitialize(reconDBProvider);
        } catch (IOException ex) {
          LOG.error("Re-initialization of task manager failed.", e);
        }
      }
    } else {
      LOG.error("Re-initialization of tasks failed.");

      // Track reprocess execution failure
      controllerMetrics.incrReprocessExecutionFailures();

      try {
        stagedReconDBProvider.close();
      } catch (Exception e) {
        LOG.error("Close of recon container staged db handler is failed", e);
      }
      recordAllTaskStatus(localReconOmTaskMap, -1, -1);
    }
    return isRunSuccessful.get();
  }

  private void recordAllTaskStatus(Map<String, ReconOmTask> localReconOmTaskMap, int status, long updateSeqNumber) {
    localReconOmTaskMap.values().forEach(task -> {
      ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(task.getTaskName());
      if (status == 0) {
        taskStatusUpdater.setLastUpdatedSeqNumber(updateSeqNumber);
      }
      taskStatusUpdater.setLastTaskRunStatus(status);
      taskStatusUpdater.recordRunCompletion();
    });

    ReconTaskStatusUpdater reprocessTaskStatus = taskStatusUpdaterManager.getTaskStatusUpdater(REPROCESS_STAGING);
    if (status == 0) {
      reprocessTaskStatus.setLastUpdatedSeqNumber(updateSeqNumber);
    }
    reprocessTaskStatus.setLastTaskRunStatus(status);
    reprocessTaskStatus.recordRunCompletion();
  }

  @Override
  public Map<String, ReconOmTask> getRegisteredTasks() {
    return reconOmTasks;
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting Recon Task Controller.");
    
    // Clean up any pre-existing checkpoint directories from previous runs
    cleanupPreExistingCheckpoints();
    
    executorService = Executors.newFixedThreadPool(threadCount,
        new ThreadFactoryBuilder().setNameFormat("ReconTaskThread-%d")
            .build());
    
    // Start async event processing thread
    eventProcessingExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("ReconEventProcessor-%d")
            .build());
    eventProcessingExecutor.submit(this::processBufferedEventsAsync);
    LOG.info("Started async event processing thread.");
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping Recon Task Controller.");
    shutdownExecutorGracefully(this.executorService, "main task executor");
    shutdownExecutorGracefully(this.eventProcessingExecutor, "event processing executor");
  }

  private void shutdownExecutorGracefully(ExecutorService executor, String name) {
    if (executor == null) {
      return;
    }
    executor.shutdown();
    try {
      if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        LOG.warn("Executor {} did not terminate within {} seconds, forcing shutdown",
            name, SHUTDOWN_TIMEOUT_SECONDS);
        executor.shutdownNow();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          LOG.error("Executor {} did not terminate after forced shutdown", name);
        }
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for {} to terminate", name);
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * For a given list of {@code Callable} tasks process them and add any failed task to the provided list.
   * The tasks are executed in parallel, but will wait for the tasks to complete i.e. the longest
   * time taken by this method will be the time taken by the longest task in the list.
   * @param tasks       A list of tasks to execute.
   * @param events      A batch of {@link OMUpdateEventBatch} events to fetch sequence number of last event in batch.
   * @param failedTasks Reference of the list to which we want to add the failed tasks for retry/reprocessing
   */
  private void processTasks(
      Collection<NamedCallableTask<ReconOmTask.TaskResult>> tasks,
      OMUpdateEventBatch events, List<ReconOmTask.TaskResult> failedTasks) {
    List<CompletableFuture<Void>> futures = tasks.stream()
        .map(task -> CompletableFuture.supplyAsync(() -> {
          // Track task delta processing duration
          long taskStartTime = Time.monotonicNow();

          try {
            ReconOmTask.TaskResult result = task.call();

            // Update task delta processing duration
            long taskDuration = Time.monotonicNow() - taskStartTime;
            taskMetrics.updateTaskDeltaProcessingDuration(task.getTaskName(), taskDuration);

            return result;
          } catch (Exception e) {
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
            // Wrap the exception with the task name
            throw new TaskExecutionException(task.getTaskName(), e);
          }
        }, executorService).thenAccept(result -> {
          String taskName = result.getTaskName();
          ReconTaskStatusUpdater taskStatusUpdater =
              taskStatusUpdaterManager.getTaskStatusUpdater(taskName);
          if (!result.isTaskSuccess()) {
            LOG.error("Task {} failed", taskName);

            // Track task delta processing failure
            taskMetrics.incrTaskDeltaProcessingFailures(taskName);

            failedTasks.add(new ReconOmTask.TaskResult.Builder()
                .setTaskName(taskName)
                .setSubTaskSeekPositions(result.getSubTaskSeekPositions())
                .build());
            taskStatusUpdater.setLastTaskRunStatus(-1);
          } else {
            // Track task delta processing success
            taskMetrics.incrTaskDeltaProcessingSuccess(taskName);

            taskStatusUpdater.setLastTaskRunStatus(0);
            taskStatusUpdater.setLastUpdatedSeqNumber(events.getLastSequenceNumber());
          }
          taskStatusUpdater.recordRunCompletion();
        }).exceptionally(ex -> {
          LOG.error("Task failed with exception: ", ex);
          if (ex.getCause() instanceof TaskExecutionException) {
            TaskExecutionException taskEx = (TaskExecutionException) ex.getCause();
            String taskName = taskEx.getTaskName();
            LOG.error("The above error occurred while trying to execute task: {}", taskName);

            // Track task delta processing failure
            taskMetrics.incrTaskDeltaProcessingFailures(taskName);

            ReconTaskStatusUpdater taskStatusUpdater =
                taskStatusUpdaterManager.getTaskStatusUpdater(taskName);
            taskStatusUpdater.setLastTaskRunStatus(-1);
            taskStatusUpdater.recordRunCompletion();
          }
          return null;
        })).collect(Collectors.toList());

    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    } catch (CompletionException ce) {
      LOG.error("Completing all tasks failed with exception ", ce);
    } catch (CancellationException ce) {
      LOG.error("Some tasks were cancelled with exception", ce);
    }
  }
  
  /**
   * Async thread that continuously processes buffered events.
   */
  private void processBufferedEventsAsync() {
    LOG.info("Started async buffered event processing thread");
    
    while (!Thread.currentThread().isInterrupted()) {
      try {
        ReconEvent event = eventBuffer.poll(1000); // 1 second timeout
        if (event != null) {
          LOG.debug("Processing buffered event of type {} with {} events", 
              event.getEventType(), event.getEventCount());
          processReconEvent(event);
        }
      } catch (Exception e) {
        LOG.error("Error in async event processing thread", e);
        // Continue processing other events
      }
    }
    
    LOG.info("Async buffered event processing thread stopped");
  }
  
  /**
   * Process a single Recon event (used by async processing thread).
   */
  @VisibleForTesting
  void processReconEvent(ReconEvent event) {
    switch (event.getEventType()) {
    case OM_UPDATE_BATCH:
      processOMUpdateBatch((OMUpdateEventBatch) event);
      break;
    case TASK_REINITIALIZATION:
      processReInitializationEvent((ReconTaskReInitializationEvent) event);
      break;
    default:
      LOG.warn("Unknown event type: {}", event.getEventType());
      break;
    }
  }
  
  /**
   * Process an OM update batch event (used by async processing thread).
   */
  private void processOMUpdateBatch(OMUpdateEventBatch events) {
    if (events.isEmpty()) {
      return;
    }

    Collection<NamedCallableTask<ReconOmTask.TaskResult>> tasks = new ArrayList<>();
    List<ReconOmTask.TaskResult> failedTasks = new ArrayList<>();

    for (Map.Entry<String, ReconOmTask> taskEntry : reconOmTasks.entrySet()) {
      ReconOmTask task = taskEntry.getValue();
      ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(task.getTaskName());
      taskStatusUpdater.recordRunStart();
      tasks.add(new NamedCallableTask<>(task.getTaskName(), () -> task.process(events, Collections.emptyMap())));
    }

    processTasks(tasks, events, failedTasks);

    // Handle failed tasks with retry logic
    List<ReconOmTask.TaskResult> retryFailedTasks = new ArrayList<>();
    if (!failedTasks.isEmpty()) {
      LOG.warn("Some tasks failed while processing buffered events, retrying...");
      tasks.clear();

      for (ReconOmTask.TaskResult taskResult : failedTasks) {
        ReconOmTask task = reconOmTasks.get(taskResult.getTaskName());
        tasks.add(new NamedCallableTask<>(task.getTaskName(),
            () -> task.process(events, taskResult.getSubTaskSeekPositions())));
      }
      processTasks(tasks, events, retryFailedTasks);

      if (!retryFailedTasks.isEmpty()) {
        LOG.warn("Some tasks still failed after retry while processing buffered events, signaling for " +
            "task reinitialization");
        // Set flag to indicate delta tasks failed even after retry
        tasksFailed.compareAndSet(false, true);
      }
    }
  }
  
  @Override
  public boolean hasEventBufferOverflowed() {
    return eventBuffer.getDroppedBatches() > 0;
  }

  /**
   * Reset the event buffer overflow flag after full snapshot is completed.
   */
  public void resetEventBufferOverflowFlag() {
    eventBuffer.resetDroppedBatches();
  }
  
  @Override
  public boolean hasTasksFailed() {
    return tasksFailed.get();
  }

  /**
   * Reset the task(s) failure flag after reinitialization is completed.
   */
  public void resetTasksFailureFlag() {
    tasksFailed.compareAndSet(true, false);
  }

  @Override
  public synchronized ReconTaskController.ReInitializationResult queueReInitializationEvent(
      ReconTaskReInitializationEvent.ReInitializationReason reason) {
    LOG.info("Queueing task reinitialization event due to: {} (retry attempt count: {})", reason,
        eventProcessRetryCount.get());

    // Track reprocess submission
    controllerMetrics.incrTotalReprocessSubmittedToQueue();

    ReInitializationResult reInitializationResult = validateRetryCountAndDelay();
    if (null != reInitializationResult) {
      return reInitializationResult;
    }

    // Drain all events in buffer and cleanup any existing checkpoints before falling back to full snapshot.
    // Events can be present in queue when reinit checkpoint creation fails multiple times because only after
    // successful creation of checkpoint, we are clearing the event buffer.
    drainEventBufferAndCleanExistingCheckpoints();

    // Try checkpoint creation (single attempt per iteration)
    ReconOMMetadataManager checkpointedOMMetadataManager = null;
    
    try {
      LOG.info("Attempting checkpoint creation (retry attempt: {})", eventProcessRetryCount.get() + 1);
      checkpointedOMMetadataManager = createOMCheckpoint(currentOMMetadataManager);
      LOG.info("Checkpoint creation succeeded");
    } catch (IOException e) {
      LOG.error("Checkpoint creation failed: {}", e.getMessage());
      handleEventFailure();
      return ReInitializationResult.RETRY_LATER;
    }

    // Create and queue the reinitialization event with checkpointed metadata manager
    ReconTaskReInitializationEvent reinitEvent =
        new ReconTaskReInitializationEvent(reason, checkpointedOMMetadataManager);
    boolean queued = eventBuffer.offer(reinitEvent);
    // If reinitialization event queued successfully, reset event buffer overflow flag and task failure flag,
    // so that we can resume queuing the delta events.
    if (queued) {
      resetEventFlags();
      // Success - reset retry counters and flags
      LOG.info("Successfully queued reinitialization event after {} retries", eventProcessRetryCount.get() + 1);
      return ReconTaskController.ReInitializationResult.SUCCESS;
    }
    return null;
  }

  private ReconTaskController.ReInitializationResult validateRetryCountAndDelay() {
    // Check if we should retry based on timing for iteration-based retries
    long currentTime = System.currentTimeMillis();
    if (eventProcessRetryCount.get() > 0) {
      // Check if 2 seconds have passed since last iteration
      long timeSinceLastRetry = currentTime - lastRetryTimestamp.get();
      if (timeSinceLastRetry < RETRY_DELAY_MS) {
        LOG.debug("Skipping retry, only {}ms since last retry attempt (need {}ms)",
            timeSinceLastRetry, RETRY_DELAY_MS);
        return ReInitializationResult.RETRY_LATER;
      }
      LOG.info("Attempting retry (retry attempt count: {}, delay: {}ms)",
          eventProcessRetryCount.get() + 1, timeSinceLastRetry);
    }
    return getEventRetryResult();
  }

  /**
   * Handle iteration failure by updating retry counters.
   */
  private void handleEventFailure() {
    long currentTime = System.currentTimeMillis();
    lastRetryTimestamp.set(currentTime);
    eventProcessRetryCount.getAndIncrement();
    tasksFailed.compareAndSet(false, true);
    LOG.error("Event processing failed {} times.", eventProcessRetryCount);
  }
  
  /**
   * Determine the appropriate retry result based on current event retry count.
   */
  private ReconTaskController.ReInitializationResult getEventRetryResult() {
    if (eventProcessRetryCount.get() >= MAX_EVENT_PROCESS_RETRIES) {
      LOG.warn("Maximum iteration retries ({}) exceeded, resetting counters and signaling full snapshot fallback",
          MAX_EVENT_PROCESS_RETRIES);
      resetRetryCounters();
      return ReconTaskController.ReInitializationResult.MAX_RETRIES_EXCEEDED;
    }
    return null;
  }

  public void drainEventBufferAndCleanExistingCheckpoints() {
    // First drain all events to check for any ReconTaskReInitializationEvent that need checkpoint cleanup
    List<ReconEvent> drainedEvents = new ArrayList<>();
    int drainedCount = eventBuffer.drainTo(drainedEvents);
    
    if (drainedCount > 0) {
      LOG.info("Drained {} events from buffer before clearing. Checking for checkpoint cleanup.", drainedCount);
      
      // Check for any ReconTaskReInitializationEvent and cleanup their checkpoints
      for (ReconEvent event : drainedEvents) {
        if (event instanceof ReconTaskReInitializationEvent) {
          ReconTaskReInitializationEvent reinitEvent = (ReconTaskReInitializationEvent) event;
          ReconOMMetadataManager checkpointedManager = reinitEvent.getCheckpointedOMMetadataManager();
          if (checkpointedManager != null) {
            LOG.info("Cleaning up unprocessed checkpoint from drained ReconTaskReInitializationEvent");
            // Close the database connections first
            try {
              checkpointedManager.close();
              LOG.debug("Closed checkpointed OM metadata manager database connections");
            } catch (Exception e) {
              LOG.warn("Failed to close checkpointed OM metadata manager", e);
            }
            // Then clean up the files
            cleanupCheckpointFiles(checkpointedManager);
          }
        }
      }
    }
  }

  @Override
  public void updateOMMetadataManager(ReconOMMetadataManager omMetadataManager) {
    LOG.debug("Updating OM metadata manager");
    this.currentOMMetadataManager = omMetadataManager;
  }

  /**
   * Create a checkpoint of the current OM metadata manager.
   * This method creates a snapshot of the current OM database state
   * to prevent data inconsistency during reinitialization.
   *
   * @param omMetaManager the OM metadata manager to checkpoint
   * @return a checkpointed ReconOMMetadataManager instance
   * @throws IOException if checkpoint creation fails
   */
  public ReconOMMetadataManager createOMCheckpoint(ReconOMMetadataManager omMetaManager)
      throws IOException {
    // Create temporary directory for checkpoint
    String parentPath = cleanTempCheckPointPath(omMetaManager);
    
    // Create checkpoint
    DBCheckpoint checkpoint = omMetaManager.getStore().getCheckpoint(parentPath, true);
    
    return omMetaManager.createCheckpointReconMetadataManager(configuration, checkpoint);
  }

  /**
   * Clean and prepare temporary checkpoint path.
   * Similar to QuotaRepairTask.cleanTempCheckPointPath.
   * 
   * @param omMetaManager the OM metadata manager
   * @return path to temporary checkpoint directory
   * @throws IOException if directory operations fail
   */
  private String cleanTempCheckPointPath(ReconOMMetadataManager omMetaManager) throws IOException {
    File dbLocation = omMetaManager.getStore().getDbLocation();
    if (dbLocation == null) {
      throw new IOException("OM DB location is null");
    }
    String tempData = dbLocation.getParent();
    if (tempData == null) {
      throw new IOException("Parent OM DB dir is null");
    }
    File reinitTmpPath =
        Paths.get(tempData, "temp-recon-reinit-checkpoint" + "_" + UUID.randomUUID()).toFile();
    FileUtils.deleteDirectory(reinitTmpPath);
    FileUtils.forceMkdir(reinitTmpPath);
    return reinitTmpPath.toString();
  }
  
  /**
   * Process a task reinitialization event asynchronously.
   */
  private void processReInitializationEvent(ReconTaskReInitializationEvent event) {
    LOG.info("Processing reinitialization event: reason={}, timestamp={}", 
        event.getReason(), event.getTimestamp());
    resetTasksFailureFlag();
    // Use the checkpointed OM metadata manager for reinitialization to prevent data inconsistency
    ReconOMMetadataManager checkpointedOMMetadataManager = null;
    try (ReconOMMetadataManager manager = event.getCheckpointedOMMetadataManager()) {
      checkpointedOMMetadataManager = manager;
      if (checkpointedOMMetadataManager != null) {
        LOG.info("Starting async task reinitialization with checkpointed OM metadata manager due to: {}",
                 event.getReason());
        boolean isRunSuccessful = reInitializeTasks(checkpointedOMMetadataManager, null);
        if (!isRunSuccessful) {
          // Setting this taskFailed flag as true here will block consuming delta events and stop buffering events
          // in eventBuffer until we successfully queue a new reinit event again.
          handleEventFailure();
          LOG.error("Task reinitialization failed, tasksFailed flag set to true");
        } else {
          resetRetryCounters();
          LOG.info("Completed async task reinitialization");
        }
      } else {
        LOG.error("Checkpointed OM metadata manager is null, cannot perform reinitialization");
        return;
      }
      LOG.info("Completed processing reinitialization event: {}", event.getReason());
    } catch (Exception e) {
      LOG.error("Error processing reinitialization event", e);
    } finally {
      if (checkpointedOMMetadataManager != null) {
        cleanupCheckpointFiles(checkpointedOMMetadataManager);
      }
    }
  }

  public void resetEventFlags() {
    // Reset appropriate flags based on the reason
    resetEventBufferOverflowFlag();
    resetTasksFailureFlag();
  }

  @Override
  public int getEventBufferSize() {
    return eventBuffer.getQueueSize();
  }

  /**
   * Get the number of batches that have been dropped due to buffer overflow.
   * This is used by the overflow detection logic.
   *
   * @return the number of dropped batches
   */
  @VisibleForTesting
  public long getDroppedBatches() {
    return eventBuffer.getDroppedBatches();
  }
  
  /**
   * Reset retry counters - for testing purposes.
   */
  @VisibleForTesting
  void resetRetryCounters() {
    eventProcessRetryCount.set(0);
    lastRetryTimestamp.set(0);
  }
  
  /**
   * Get current iteration retry count - for testing purposes.
   */
  @VisibleForTesting  
  int getEventProcessRetryCount() {
    return eventProcessRetryCount.get();
  }
  
  /**
   * Get tasksFailed flag - for testing purposes.
   */
  @VisibleForTesting
  AtomicBoolean getTasksFailedFlag() {
    return tasksFailed;
  }
  
  /**
   * Clean up any pre-existing checkpoint directories from previous runs.
   * This method looks for and removes any leftover temporary checkpoint directories
   * that may not have been cleaned up properly during previous shutdowns.
   */
  private void cleanupPreExistingCheckpoints() {
    try {
      if (currentOMMetadataManager == null) {
        LOG.debug("No current OM metadata manager, skipping pre-existing checkpoint cleanup");
        return;
      }
      
      // Get the base directory where checkpoints are created
      File dbLocation = currentOMMetadataManager.getStore().getDbLocation();
      if (dbLocation == null || dbLocation.getParent() == null) {
        LOG.debug("Cannot determine checkpoint base directory, skipping pre-existing checkpoint cleanup");
        return;
      }
      
      String baseDirectory = dbLocation.getParent();
      File baseDir = new File(baseDirectory);
      
      if (!baseDir.exists() || !baseDir.isDirectory()) {
        LOG.debug("Base directory {} does not exist, skipping pre-existing checkpoint cleanup", baseDirectory);
        return;
      }
      
      // Look for temporary checkpoint directories matching our naming pattern
      File[] checkpointDirs = baseDir.listFiles((dir, name) -> 
          name.startsWith("temp-recon-reinit-checkpoint"));
      
      if (checkpointDirs != null && checkpointDirs.length > 0) {
        LOG.info("Found {} pre-existing checkpoint directories to clean up", checkpointDirs.length);
        
        for (File checkpointDir : checkpointDirs) {
          try {
            if (checkpointDir.exists() && checkpointDir.isDirectory()) {
              FileUtils.deleteDirectory(checkpointDir);
              LOG.info("Cleaned up pre-existing checkpoint directory: {}", checkpointDir);
            }
          } catch (IOException e) {
            LOG.warn("Failed to clean up pre-existing checkpoint directory: {}", checkpointDir, e);
          }
        }
      } else {
        LOG.debug("No pre-existing checkpoint directories found");
      }
      
    } catch (Exception e) {
      LOG.warn("Failed to cleanup pre-existing checkpoint directories", e);
    }
  }
  
  /**
   * Cleanup checkpoint files for a checkpointed OM metadata manager.
   * This method only removes the temporary checkpoint files without closing database connections.
   * Used when the manager is closed via try-with-resources.
   * 
   * @param checkpointedManager the checkpointed OM metadata manager
   */
  private void cleanupCheckpointFiles(ReconOMMetadataManager checkpointedManager) {
    if (checkpointedManager == null) {
      return;
    }
    try {
      // Get the checkpoint location
      File checkpointLocation = null;
      try {
        if (checkpointedManager.getStore() != null && 
            checkpointedManager.getStore().getDbLocation() != null) {
          // The checkpoint location is typically the parent directory of the DB location
          checkpointLocation = checkpointedManager.getStore().getDbLocation().getParentFile();
        }
      } catch (Exception e) {
        LOG.warn("Failed to get checkpoint location for cleanup", e);
      }
      
      // Clean up the checkpoint files if we have the location
      if (checkpointLocation != null && checkpointLocation.exists()) {
        try {
          FileUtils.deleteDirectory(checkpointLocation);
          LOG.debug("Cleaned up checkpoint directory: {}", checkpointLocation);
        } catch (IOException e) {
          LOG.warn("Failed to cleanup checkpoint directory: {}", checkpointLocation, e);
        }
      }
      
    } catch (Exception e) {
      LOG.warn("Failed to cleanup checkpoint files", e);
    }
  }

  @VisibleForTesting
  public OMUpdateEventBuffer getEventBuffer() {
    return eventBuffer;
  }
}
