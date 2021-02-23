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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
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
  private ReconTaskStatusDao reconTaskStatusDao;

  @Inject
  public ReconTaskControllerImpl(OzoneConfiguration configuration,
                                 ReconTaskStatusDao reconTaskStatusDao,
                                 Set<ReconOmTask> tasks) {
    reconOmTasks = new HashMap<>();
    threadCount = configuration.getInt(OZONE_RECON_TASK_THREAD_COUNT_KEY,
        OZONE_RECON_TASK_THREAD_COUNT_DEFAULT);
    this.reconTaskStatusDao = reconTaskStatusDao;
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
    // Create DB record for the task.
    ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(taskName,
        0L, 0L);
    if (!reconTaskStatusDao.existsById(taskName)) {
      reconTaskStatusDao.insert(reconTaskStatusRecord);
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
        for (Map.Entry<String, ReconOmTask> taskEntry :
            reconOmTasks.entrySet()) {
          ReconOmTask task = taskEntry.getValue();
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
    try {
      Collection<Callable<Pair<String, Boolean>>> tasks = new ArrayList<>();
      for (Map.Entry<String, ReconOmTask> taskEntry :
          reconOmTasks.entrySet()) {
        ReconOmTask task = taskEntry.getValue();
        tasks.add(() -> task.reprocess(omMetadataManager));
      }
      List<Future<Pair<String, Boolean>>> results =
          executorService.invokeAll(tasks);
      for (Future<Pair<String, Boolean>> f : results) {
        String taskName = f.get().getLeft();
        if (!f.get().getRight()) {
          LOG.info("Init failed for task {}.", taskName);
        } else {
          //store the timestamp for the task
          ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(taskName,
              System.currentTimeMillis(),
              omMetadataManager.getLastSequenceNumberFromDB());
          reconTaskStatusDao.update(reconTaskStatusRecord);
        }
      }
    } catch (ExecutionException e) {
      LOG.error("Unexpected error : ", e);
    }
  }

  /**
   * Store the last completed event sequence number and timestamp to the DB
   * for that task.
   * @param taskName taskname to be updated.
   * @param lastSequenceNumber contains the new sequence number.
   */
  private void storeLastCompletedTransaction(
      String taskName, long lastSequenceNumber) {
    ReconTaskStatus reconTaskStatusRecord = new ReconTaskStatus(taskName,
        System.currentTimeMillis(), lastSequenceNumber);
    reconTaskStatusDao.update(reconTaskStatusRecord);
  }

  @Override
  public Map<String, ReconOmTask> getRegisteredTasks() {
    return reconOmTasks;
  }

  @Override
  public ReconTaskStatusDao getReconTaskStatusDao() {
    return reconTaskStatusDao;
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting Recon Task Controller.");
    executorService = Executors.newFixedThreadPool(threadCount);
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
      if (!f.get().getRight()) {
        LOG.info("Failed task : {}", taskName);
        failedTasks.add(f.get().getLeft());
      } else {
        taskFailureCounter.get(taskName).set(0);
        storeLastCompletedTransaction(taskName, events.getLastSequenceNumber());
      }
    }
    return failedTasks;
  }
}
