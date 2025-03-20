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

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_THREAD_COUNT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_THREAD_COUNT_KEY;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
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

  private Map<String, ReconOmTask> reconOmTasks;
  private ExecutorService executorService;
  private final int threadCount;
  private Map<String, AtomicInteger> taskFailureCounter = new HashMap<>();
  private static final int TASK_FAILURE_THRESHOLD = 2;
  private final ReconTaskStatusUpdaterManager taskStatusUpdaterManager;

  @Inject
  public ReconTaskControllerImpl(OzoneConfiguration configuration,
                                 Set<ReconOmTask> tasks,
                                 ReconTaskStatusUpdaterManager taskStatusUpdaterManager) {
    reconOmTasks = new HashMap<>();
    threadCount = configuration.getInt(OZONE_RECON_TASK_THREAD_COUNT_KEY,
        OZONE_RECON_TASK_THREAD_COUNT_DEFAULT);
    this.taskStatusUpdaterManager = taskStatusUpdaterManager;
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
    // Store Task in Task failure tracker.
    taskFailureCounter.put(taskName, new AtomicInteger(0));
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
      Collection<NamedCallableTask<ReconOmTask.TaskResult>> tasks = new ArrayList<>();
      List<ReconOmTask.TaskResult> failedTasks = new ArrayList<>();
      for (Map.Entry<String, ReconOmTask> taskEntry :
          reconOmTasks.entrySet()) {
        ReconOmTask task = taskEntry.getValue();
        ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(task.getTaskName());
        taskStatusUpdater.recordRunStart();
        // events passed to process method is no longer filtered
        tasks.add(new NamedCallableTask<>(task.getTaskName(), () -> task.process(events, Collections.emptyMap())));
      }
      processTasks(tasks, events, failedTasks);

      // Retry processing failed tasks
      List<ReconOmTask.TaskResult> retryFailedTasks = new ArrayList<>();
      if (!failedTasks.isEmpty()) {
        tasks.clear();
        for (ReconOmTask.TaskResult taskResult : failedTasks) {
          ReconOmTask task = reconOmTasks.get(taskResult.getTaskName());
          // events passed to process method is no longer filtered
          tasks.add(new NamedCallableTask<>(task.getTaskName(),
              () -> task.process(events, taskResult.getSubTaskSeekPositions())));
        }
        processTasks(tasks, events, retryFailedTasks);
      }

      // Reprocess the failed tasks.
      ReconConstants.resetTableTruncatedFlags();
      if (!retryFailedTasks.isEmpty()) {
        tasks.clear();
        for (ReconOmTask.TaskResult taskResult : failedTasks) {
          ReconOmTask task = reconOmTasks.get(taskResult.getTaskName());
          tasks.add(new NamedCallableTask<>(task.getTaskName(), () -> task.reprocess(omMetadataManager)));
        }
        List<ReconOmTask.TaskResult> reprocessFailedTasks = new ArrayList<>();
        processTasks(tasks, events, reprocessFailedTasks);
        // Here the assumption is that even if full re-process of task also fails,
        // then there is something wrong in recon rocks DB got from OM and needs to be
        // investigated.
        ignoreFailedTasks(reprocessFailedTasks);
      }
    }
  }

  /**
   * Ignore tasks that failed reprocess step more than threshold times.
   * @param failedTasks list of failed tasks.
   */
  private void ignoreFailedTasks(List<ReconOmTask.TaskResult> failedTasks) {
    for (ReconOmTask.TaskResult taskResult : failedTasks) {
      String taskName = taskResult.getTaskName();
      LOG.info("Reprocess step failed for task {}.", taskName);
      if (taskFailureCounter.get(taskName).incrementAndGet() >
          TASK_FAILURE_THRESHOLD) {
        LOG.info("Ignoring task since it failed retry and " +
            "reprocess more than {} times.", TASK_FAILURE_THRESHOLD);
        reconOmTasks.remove(taskName);
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
    Collection<NamedCallableTask<ReconOmTask.TaskResult>> tasks = new ArrayList<>();
    Map<String, ReconOmTask> localReconOmTaskMap = reconOmTaskMap;
    if (reconOmTaskMap == null) {
      localReconOmTaskMap = reconOmTasks;
    }
    ReconConstants.resetTableTruncatedFlags();

    localReconOmTaskMap.values().forEach(task -> {
      ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(task.getTaskName());
      taskStatusUpdater.recordRunStart();
      tasks.add(new NamedCallableTask<>(task.getTaskName(), () -> task.reprocess(omMetadataManager)));
    });

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
            String taskName = result.getTaskName();
            ReconTaskStatusUpdater taskStatusUpdater =
                taskStatusUpdaterManager.getTaskStatusUpdater(taskName);
            if (!result.isTaskSuccess()) {
              LOG.error("Init failed for task {}.", taskName);
              taskStatusUpdater.setLastTaskRunStatus(-1);
            } else {
              taskStatusUpdater.setLastTaskRunStatus(0);
              taskStatusUpdater.setLastUpdatedSeqNumber(
                  omMetadataManager.getLastSequenceNumberFromDB());
            }
            taskStatusUpdater.recordRunCompletion();
          }).exceptionally(ex -> {
            LOG.error("Task failed with exception: ", ex);
            if (ex.getCause() instanceof TaskExecutionException) {
              TaskExecutionException taskEx = (TaskExecutionException) ex.getCause();
              String taskName = taskEx.getTaskName();
              LOG.error("The above error occurred while trying to execute task: {}", taskName);

              ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(taskName);
              taskStatusUpdater.setLastTaskRunStatus(-1);
              taskStatusUpdater.recordRunCompletion();
            }
            return null;
          })).toArray(CompletableFuture[]::new)).join();
    } catch (CompletionException ce) {
      LOG.error("Completing all tasks failed with exception ", ce);
    } catch (CancellationException ce) {
      LOG.error("Some tasks were cancelled with exception", ce);
    }
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
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping Recon Task Controller.");
    if (this.executorService != null) {
      this.executorService.shutdownNow();
    }
  }

  /**
   * For a given list of {@link Callable} tasks process them and add any failed task to the provided list.
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
            taskFailureCounter.get(taskName).set(0);
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
}
