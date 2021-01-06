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
package org.apache.hadoop.ozone.container.replication;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.replication.ReplicationTask.Status;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single point to schedule the downloading tasks based on priorities.
 */
public class ReplicationSupervisor {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicationSupervisor.class);

  private final ContainerSet containerSet;
  private final ContainerReplicator replicator;
  private final ExecutorService executor;

  private final AtomicLong requestCounter = new AtomicLong();
  private final AtomicLong successCounter = new AtomicLong();
  private final AtomicLong failureCounter = new AtomicLong();

  /**
   * A set of container IDs that are currently being downloaded
   * or queued for download. Tracked so we don't schedule > 1
   * concurrent download for the same container.
   */
  private final KeySetView<Object, Boolean> containersInFlight;

  @VisibleForTesting
  ReplicationSupervisor(
      ContainerSet containerSet, ContainerReplicator replicator,
      ExecutorService executor
  ) {
    this.containerSet = containerSet;
    this.replicator = replicator;
    this.containersInFlight = ConcurrentHashMap.newKeySet();
    this.executor = executor;
  }

  public ReplicationSupervisor(
      ContainerSet containerSet,
      ContainerReplicator replicator, int poolSize
  ) {
    this(containerSet, replicator, new ThreadPoolExecutor(
        poolSize, poolSize, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("ContainerReplicationThread-%d")
            .build()));
  }

  /**
   * Queue an asynchronous download of the given container.
   */
  public void addTask(ReplicationTask task) {
    if (containersInFlight.add(task.getContainerId())) {
      executor.execute(new TaskRunner(task));
    }
  }

  @VisibleForTesting
  public void shutdownAfterFinish() throws InterruptedException {
    executor.shutdown();
    executor.awaitTermination(1L, TimeUnit.DAYS);
  }

  public void stop() {
    try {
      executor.shutdown();
      if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException ie) {
      // Ignore, we don't really care about the failure.
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Get the number of containers currently being downloaded
   * or scheduled for download.
   *
   * @return Count of in-flight replications.
   */
  @VisibleForTesting
  int getInFlightReplications() {
    return containersInFlight.size();
  }

  public final class TaskRunner implements Runnable {
    private final ReplicationTask task;

    public TaskRunner(ReplicationTask task) {
      this.task = task;
    }

    @Override
    public void run() {
      final Long containerId = task.getContainerId();
      try {
        requestCounter.incrementAndGet();

        if (containerSet.getContainer(task.getContainerId()) != null) {
          LOG.debug("Container {} has already been downloaded.", containerId);
          return;
        }

        task.setStatus(Status.DOWNLOADING);
        replicator.replicate(task);

        if (task.getStatus() == Status.FAILED) {
          LOG.error(
              "Container {} can't be downloaded from any of the datanodes.",
              containerId);
          failureCounter.incrementAndGet();
        } else if (task.getStatus() == Status.DONE) {
          LOG.info("Container {} is replicated.", containerId);
          successCounter.incrementAndGet();
        }
      } catch (Exception e) {
        task.setStatus(Status.FAILED);
        LOG.error("Encountered error while replicating container {}.",
            containerId, e);
        failureCounter.incrementAndGet();
      } finally {
        containersInFlight.remove(containerId);
      }
    }
  }

  public long getReplicationRequestCount() {
    return requestCounter.get();
  }

  public long getReplicationSuccessCount() {
    return successCounter.get();
  }

  public long getReplicationFailureCount() {
    return failureCounter.get();
  }
}
