/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_THREAD_COUNT_DEFAULT;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_TASK_THREAD_COUNT_KEY;

import java.util.ArrayList;
import java.util.Collection;
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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.types.NamedCallableTask;
import org.apache.hadoop.ozone.recon.tasks.types.TaskExecutionException;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

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
      Collection<NamedCallableTask<Pair<String, Boolean>>> tasks = new ArrayList<>();
      List<String> failedTasks = new ArrayList<>();
      for (Map.Entry<String, ReconOmTask> taskEntry :
          reconOmTasks.entrySet()) {
        ReconOmTask task = taskEntry.getValue();
        ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(task.getTaskName());
        taskStatusUpdater.recordRunStart();
        // events passed to process method is no longer filtered
        tasks.add(new NamedCallableTask<>(task.getTaskName(), () -> task.process(events)));
      }
      processTasks(tasks, events, failedTasks);

      // Retry processing failed tasks
      List<String> retryFailedTasks = new ArrayList<>();
      if (!failedTasks.isEmpty()) {
        tasks.clear();
        for (String taskName : failedTasks) {
          ReconOmTask task = reconOmTasks.get(taskName);
          // events passed to process method is no longer filtered
          tasks.add(new NamedCallableTask<>(task.getTaskName(),
              () -> task.process(events)));
        }
        processTasks(tasks, events, retryFailedTasks);
      }

      // Reprocess the failed tasks.
      if (!retryFailedTasks.isEmpty()) {
        tasks.clear();
        for (String taskName : failedTasks) {
          ReconOmTask task = reconOmTasks.get(taskName);
          tasks.add(new NamedCallableTask<>(task.getTaskName(), () -> task.reprocess(omMetadataManager)));
        }
        List<String> reprocessFailedTasks = new ArrayList<>();
        processTasks(tasks, events, reprocessFailedTasks);
        ignoreFailedTasks(reprocessFailedTasks);
      }
    }
  }

  /**
   * Ignore tasks that failed reprocess step more than threshold times.
   * @param failedTasks list of failed tasks.
   */
  private void ignoreFailedTasks(List<String> failedTasks) {
    for (String taskName : failedTasks) {
      LOG.info("Reprocess step failed for task {}.", taskName);
      if (taskFailureCounter.get(taskName).incrementAndGet() >
          TASK_FAILURE_THRESHOLD) {
        LOG.info("Ignoring task since it failed retry and " +
            "reprocess more than {} times.", TASK_FAILURE_THRESHOLD);
        reconOmTasks.remove(taskName);
      }
    }
  }

  @Override
  public synchronized void reInitializeTasks(ReconOMMetadataManager omMetadataManager) {
    Collection<NamedCallableTask<Pair<String, Boolean>>> tasks = new ArrayList<>();
    for (Map.Entry<String, ReconOmTask> taskEntry :
        reconOmTasks.entrySet()) {
      ReconOmTask task = taskEntry.getValue();
      ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(task.getTaskName());
      taskStatusUpdater.recordRunStart();
      tasks.add(new NamedCallableTask<>(task.getTaskName(), () -> task.reprocess(omMetadataManager)));
    }

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
            String taskName = result.getLeft();
            ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(taskName);
            if (!result.getRight()) {
              LOG.error("Init failed for task {}.", taskName);
              taskStatusUpdater.setLastTaskRunStatus(-1);
            } else {
              taskStatusUpdater.setLastTaskRunStatus(0);
              taskStatusUpdater.setLastUpdatedSeqNumber(omMetadataManager.getLastSequenceNumberFromDB());
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
  private void processTasks(Collection<NamedCallableTask<Pair<String, Boolean>>> tasks,
                            OMUpdateEventBatch events, List<String> failedTasks) {
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
          String taskName = result.getLeft();
          ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(taskName);
          if (!result.getRight()) {
            LOG.error("Task {} failed", taskName);
            failedTasks.add(result.getLeft());
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

            ReconTaskStatusUpdater taskStatusUpdater = taskStatusUpdaterManager.getTaskStatusUpdater(taskName);
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
