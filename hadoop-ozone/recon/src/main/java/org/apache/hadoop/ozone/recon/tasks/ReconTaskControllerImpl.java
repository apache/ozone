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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.metrics.ReconTaskStatusCounter;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
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
  private final ReconTaskStatusDao reconTaskStatusDao;
  private final ReconTaskStatusCounter reconTaskStatusCounter;
  private final Map<String, ReconTaskStatusUpdater> taskStatusMap = new ConcurrentHashMap<>();

  @Inject
  public ReconTaskControllerImpl(OzoneConfiguration configuration,
                                 Set<ReconOmTask> tasks,
                                 ReconTaskStatusDao reconTaskStatusDao,
                                 ReconTaskStatusCounter reconTaskStatusCounter) {
    reconOmTasks = new HashMap<>();
    threadCount = configuration.getInt(OZONE_RECON_TASK_THREAD_COUNT_KEY,
        OZONE_RECON_TASK_THREAD_COUNT_DEFAULT);
    this.reconTaskStatusDao = reconTaskStatusDao;
    this.reconTaskStatusCounter = reconTaskStatusCounter;
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
    try {
      // Create DB record for the task.
      taskStatusMap.put(taskName, new ReconTaskStatusUpdater(reconTaskStatusDao, reconTaskStatusCounter, taskName));
      getUpdaterForTask(taskName).updateDetails();
    } catch (Exception e) {
      LOG.error("Caught exception while registering task: {}, {}", taskName, e.getMessage());
    }
  }

  /**
   * For every registered task, we try process step twice and then reprocess
   * once (if process failed twice) to absorb the events. If a task has failed
   * reprocess call more than 2 times across events, it is unregistered
   * (ignored).
   * @param events set of events
   * @throws InterruptedException
   */
  @Override
  public synchronized void consumeOMEvents(OMUpdateEventBatch events,
                              OMMetadataManager omMetadataManager)
      throws InterruptedException {

    try {
      if (!events.isEmpty()) {
        Collection<Callable<Pair<String, Boolean>>> tasks = new ArrayList<>();
        ReconTaskStatus reconTaskStatus;
        for (Map.Entry<String, ReconOmTask> taskEntry :
            reconOmTasks.entrySet()) {
          ReconOmTask task = taskEntry.getValue();
          ReconTaskStatusUpdater taskStatusUpdater = getUpdaterForTask(task.getTaskName());
          taskStatusUpdater.setIsCurrentTaskRunning(1);
          taskStatusUpdater.setLastUpdatedTimestamp(System.currentTimeMillis());
          taskStatusUpdater.updateDetails();
          // events passed to process method is no longer filtered
          tasks.add(() -> task.process(events));
        }

        List<Future<Pair<String, Boolean>>> results =
            executorService.invokeAll(tasks);
        List<String> failedTasks = processTaskResults(results, events);

        // Retry
        List<String> retryFailedTasks = new ArrayList<>();
        if (!failedTasks.isEmpty()) {
          tasks.clear();
          for (String taskName : failedTasks) {
            ReconOmTask task = reconOmTasks.get(taskName);
            // events passed to process method is no longer filtered
            tasks.add(() -> task.process(events));
          }
          results = executorService.invokeAll(tasks);
          retryFailedTasks = processTaskResults(results, events);
        }

        // Reprocess the failed tasks.
        if (!retryFailedTasks.isEmpty()) {
          tasks.clear();
          for (String taskName : failedTasks) {
            ReconOmTask task = reconOmTasks.get(taskName);
            tasks.add(() -> task.reprocess(omMetadataManager));
          }
          results = executorService.invokeAll(tasks);
          List<String> reprocessFailedTasks =
              processTaskResults(results, events);
          ignoreFailedTasks(reprocessFailedTasks);
        }
      }
    } catch (ExecutionException e) {
      LOG.error("Unexpected error : ", e);
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
  public synchronized void reInitializeTasks(
      ReconOMMetadataManager omMetadataManager) throws InterruptedException {
      Collection<Callable<Pair<String, Boolean>>> tasks = new ArrayList<>();
      for (Map.Entry<String, ReconOmTask> taskEntry :
          reconOmTasks.entrySet()) {
        ReconOmTask task = taskEntry.getValue();
        ReconTaskStatusUpdater taskStatusUpdater = getUpdaterForTask(task.getTaskName());
        taskStatusUpdater.setIsCurrentTaskRunning(1);
        taskStatusUpdater.setLastUpdatedTimestamp(System.currentTimeMillis());
        taskStatusUpdater.updateDetails();
        tasks.add(() -> task.reprocess(omMetadataManager));
      }
      List<Future<Pair<String, Boolean>>> results =
          executorService.invokeAll(tasks);
      for (Future<Pair<String, Boolean>> f : results) {
        try {
          String taskName = f.get().getLeft();
          ReconTaskStatusUpdater taskStatusUpdater = getUpdaterForTask(taskName);
          taskStatusUpdater.setLastUpdatedSeqNumber(omMetadataManager.getLastSequenceNumberFromDB());
          taskStatusUpdater.setLastUpdatedTimestamp(System.currentTimeMillis());
          if (!f.get().getRight()) {
            LOG.info("Init failed for task {}.", taskName);
            taskStatusUpdater.setLastTaskRunStatus(-1);
          } else {
            //store the timestamp for the task
            taskStatusUpdater.setLastTaskRunStatus(0);
          }
          taskStatusUpdater.setIsCurrentTaskRunning(0);
          taskStatusUpdater.updateDetails();
        } catch (ExecutionException e) {
          LOG.error("Unexpected error : ", e);
        }
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
   * Wait on results of all tasks.
   * @param results Set of Futures.
   * @param events Events.
   * @return List of failed task names
   * @throws ExecutionException execution Exception
   * @throws InterruptedException Interrupted Exception
   */
  private List<String> processTaskResults(List<Future<Pair<String, Boolean>>>
                                              results,
                                          OMUpdateEventBatch events)
      throws ExecutionException, InterruptedException {
    List<String> failedTasks = new ArrayList<>();
    for (Future<Pair<String, Boolean>> f : results) {
      String taskName = f.get().getLeft();
      ReconTaskStatusUpdater taskStatusUpdater = getUpdaterForTask(taskName);
      taskStatusUpdater.setLastUpdatedSeqNumber(events.getLastSequenceNumber());
      taskStatusUpdater.setLastUpdatedTimestamp(System.currentTimeMillis());
      taskStatusUpdater.setIsCurrentTaskRunning(0);

      if (!f.get().getRight()) {
        LOG.info("Failed task : {}", taskName);
        failedTasks.add(f.get().getLeft());
        taskStatusUpdater.setLastTaskRunStatus(-1);
      } else {
        taskFailureCounter.get(taskName).set(0);
        taskStatusUpdater.setLastTaskRunStatus(0);
      }
      taskStatusUpdater.updateDetails();
    }
    return failedTasks;
  }

  /**
   * Get task status update builder for a task name from taskStatusMap
   * @param taskName Stores the task name for which we want the builder
   * @return A {@link ReconTaskStatusUpdater} instance of the current task
   */
  private ReconTaskStatusUpdater getUpdaterForTask(String taskName) {
    return taskStatusMap.getOrDefault(taskName, new ReconTaskStatusUpdater(
        reconTaskStatusDao, reconTaskStatusCounter, taskName));
  }
}
