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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.apache.hadoop.ozone.recon.tasks.types.NamedCallableTask;
import org.apache.hadoop.ozone.recon.tasks.types.TaskExecutionException;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of ReconTaskController.
 */
public class ReconTaskControllerImpl implements ReconTaskController {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconTaskControllerImpl.class);
  private static final String REPROCESS_STAGING = "REPROCESS_STAGING";
  private final ReconDBProvider reconDBProvider;
  private final ReconContainerMetadataManager reconContainerMetadataManager;
  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  private Map<String, ReconOmTask> reconOmTasks;
  private ExecutorService executorService;
  private final int threadCount;
  private final ReconTaskStatusUpdaterManager taskStatusUpdaterManager;
  private final OMUpdateEventBuffer eventBuffer;
  private ExecutorService eventProcessingExecutor;
  private final AtomicBoolean deltaTasksFailed = new AtomicBoolean(false);
  private volatile ReconOMMetadataManager currentOMMetadataManager;

  @Inject
  public ReconTaskControllerImpl(OzoneConfiguration configuration,
                                 Set<ReconOmTask> tasks,
                                 ReconTaskStatusUpdaterManager taskStatusUpdaterManager,
                                 ReconDBProvider reconDBProvider,
                                 ReconContainerMetadataManager reconContainerMetadataManager,
                                 ReconNamespaceSummaryManager reconNamespaceSummaryManager) {
    this.reconDBProvider = reconDBProvider;
    this.reconContainerMetadataManager = reconContainerMetadataManager;
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    reconOmTasks = new HashMap<>();
    threadCount = configuration.getInt(OZONE_RECON_TASK_THREAD_COUNT_KEY,
        OZONE_RECON_TASK_THREAD_COUNT_DEFAULT);
    this.taskStatusUpdaterManager = taskStatusUpdaterManager;
    int eventBufferCapacity = configuration.getInt(OZONE_RECON_OM_EVENT_BUFFER_CAPACITY,
        OZONE_RECON_OM_EVENT_BUFFER_CAPACITY_DEFAULT);
    this.eventBuffer = new OMUpdateEventBuffer(eventBufferCapacity);
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
    if (!events.isEmpty()) {
      // Always buffer events for async processing
      boolean buffered = eventBuffer.offer(events);
      if (!buffered) {
        LOG.error("Event buffer is full (capacity: {}). Dropping buffered events and signaling full snapshot. " +
            "Buffer size: {}, Dropped batches: {}", 
            20000, eventBuffer.getQueueSize(), eventBuffer.getDroppedBatches());
        
        // Clear buffer and signal full snapshot requirement
        eventBuffer.clear();
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
   * @param reconOmTaskMap a map of Recon OM tasks whose lastUpdatedSeqNumber does not match
   *                       the lastUpdatedSeqNumber from the previous run of the 'OmDeltaRequest' task.
   *                       These tasks will be reinitialized to process the delta OM DB updates
   *                       received in the last run of 'OmDeltaRequest'.
   *                       If {@code reconOmTaskMap} is null, all registered Recon OM tasks
   *                       will be reinitialized.
   */
  @Override
  public synchronized void reInitializeTasks(ReconOMMetadataManager omMetadataManager,
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
      recordAllTaskStatus(localReconOmTaskMap, -1, -1);
      return;
    }

    localReconOmTaskMap.values().forEach(task -> {
      ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(task.getTaskName());
      taskStatusUpdater.recordRunStart();
      tasks.add(new NamedCallableTask<>(task.getTaskName(),
          () -> task.getStagedTask(omMetadataManager, stagedReconDBProvider.getDbStore())
              .reprocess(omMetadataManager)));
    });

    AtomicBoolean isRunSuccessful = new AtomicBoolean(true);
    try {
      CompletableFuture.allOf(tasks.stream()
          .map(task -> CompletableFuture.supplyAsync(() -> {
            try {
              return task.call();
            } catch (Exception e) {
              if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
              }
              // Wrap the exception with the task name
              throw new TaskExecutionException(task.getTaskName(), e);
            }
          }, executorService).thenAccept(result -> {
            if (!result.isTaskSuccess()) {
              String taskName = result.getTaskName();
              LOG.error("Init failed for task {}.", taskName);
              isRunSuccessful.set(false);
            }
          }).exceptionally(ex -> {
            LOG.error("Task failed with exception: ", ex);
            isRunSuccessful.set(false);
            if (ex.getCause() instanceof TaskExecutionException) {
              TaskExecutionException taskEx = (TaskExecutionException) ex.getCause();
              String taskName = taskEx.getTaskName();
              LOG.error("The above error occurred while trying to execute task: {}", taskName);
            }
            return null;
          })).toArray(CompletableFuture[]::new)).join();
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
        recordAllTaskStatus(localReconOmTaskMap, 0, omMetadataManager.getLastSequenceNumberFromDB());
        LOG.info("Re-initialization of tasks completed successfully.");
      } catch (Exception e) {
        LOG.error("Re-initialization of tasks failed.", e);
        recordAllTaskStatus(localReconOmTaskMap, -1, -1);
        // reinitialize the Recon OM tasks with the original DB provider
        try {
          reconNamespaceSummaryManager.reinitialize(reconDBProvider);
          reconContainerMetadataManager.reinitialize(reconDBProvider);
        } catch (IOException ex) {
          LOG.error("Re-initialization of task manager failed.", e);
        }
      }
    } else {
      LOG.error("Re-initialization of tasks failed.");
      try {
        stagedReconDBProvider.close();
      } catch (Exception e) {
        LOG.error("Close of recon container staged db handler is failed", e);
      }
      recordAllTaskStatus(localReconOmTaskMap, -1, -1);
    }
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
    if (this.executorService != null) {
      this.executorService.shutdownNow();
    }
    if (this.eventProcessingExecutor != null) {
      this.eventProcessingExecutor.shutdownNow();
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
          try {
            return task.call();
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
            failedTasks.add(new ReconOmTask.TaskResult.Builder()
                .setTaskName(taskName)
                .setSubTaskSeekPositions(result.getSubTaskSeekPositions())
                .build());
            taskStatusUpdater.setLastTaskRunStatus(-1);
          } else {
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
  private void processReconEvent(ReconEvent event) {
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
        deltaTasksFailed.set(true);
      }
    }
  }
  
  @Override
  public boolean hasEventBufferOverflowed() {
    return eventBuffer.getDroppedBatches() > 0;
  }
  
  @Override
  public void resetEventBufferOverflowFlag() {
    eventBuffer.resetDroppedBatches();
  }
  
  @Override
  public boolean hasDeltaTasksFailed() {
    return deltaTasksFailed.get();
  }
  
  @Override
  public void resetDeltaTasksFailureFlag() {
    deltaTasksFailed.set(false);
  }
  
  @Override
  public boolean queueReInitializationEvent(ReconTaskReInitializationEvent.ReInitializationReason reason) {
    LOG.info("Queueing task reinitialization event due to: {}", reason);
    
    // Clear the buffer to discard all pending events
    eventBuffer.clear();
    
    // Create and queue the reinitialization event
    ReconTaskReInitializationEvent reinitEvent = new ReconTaskReInitializationEvent(reason);
    boolean queued = eventBuffer.offer(reinitEvent);
    
    if (queued) {
      LOG.info("Successfully queued reinitialization event");
    } else {
      LOG.error("Failed to queue reinitialization event - buffer is full");
    }
    
    return queued;
  }
  
  @Override
  public void updateOMMetadataManager(ReconOMMetadataManager omMetadataManager) {
    this.currentOMMetadataManager = omMetadataManager;
  }
  
  /**
   * Process a task reinitialization event asynchronously.
   */
  private void processReInitializationEvent(ReconTaskReInitializationEvent event) {
    LOG.info("Processing reinitialization event: reason={}, timestamp={}", 
        event.getReason(), event.getTimestamp());
    
    try {
      // Use the current OM metadata manager for reinitialization
      if (currentOMMetadataManager != null) {
        LOG.info("Starting async task reinitialization due to: {}", event.getReason());
        reInitializeTasks(currentOMMetadataManager, null);
        LOG.info("Completed async task reinitialization");
      } else {
        LOG.warn("Current OM metadata manager is null, cannot perform reinitialization");
        deltaTasksFailed.set(true);
        return;
      }
      
      // Reset appropriate flags based on the reason
      if (event.getReason() == ReconTaskReInitializationEvent.ReInitializationReason.BUFFER_OVERFLOW) {
        resetEventBufferOverflowFlag();
      } else if (event.getReason() == ReconTaskReInitializationEvent.ReInitializationReason.TASK_FAILURES) {
        resetDeltaTasksFailureFlag();
      }
      
      LOG.info("Completed processing reinitialization event: {}", event.getReason());
      
    } catch (Exception e) {
      LOG.error("Error processing reinitialization event", e);
      // Set failure flags so the sync thread can try again
      deltaTasksFailed.set(true);
    }
  }
}
