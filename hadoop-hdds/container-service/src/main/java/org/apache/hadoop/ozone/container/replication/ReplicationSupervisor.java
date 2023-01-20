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

import java.time.Clock;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig;
import org.apache.hadoop.ozone.container.replication.AbstractReplicationTask.Status;

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
  private final ContainerReplicator pullReplicator;
  private final ContainerReplicator pushReplicator;
  private final ExecutorService executor;
  private final StateContext context;
  private final Clock clock;

  private final AtomicLong requestCounter = new AtomicLong();
  private final AtomicLong successCounter = new AtomicLong();
  private final AtomicLong failureCounter = new AtomicLong();
  private final AtomicLong timeoutCounter = new AtomicLong();

  /**
   * A set of container IDs that are currently being downloaded
   * or queued for download. Tracked so we don't schedule > 1
   * concurrent download for the same container.
   */
  private final Set<ReplicationTask> inFlight;

  @VisibleForTesting
  ReplicationSupervisor(
      ContainerSet containerSet, StateContext context,
      ContainerReplicator pullReplicator, ContainerReplicator pushReplicator,
      ExecutorService executor,
      Clock clock) {
    this.containerSet = containerSet;
    this.pullReplicator = pullReplicator;
    this.pushReplicator = pushReplicator;
    this.inFlight = ConcurrentHashMap.newKeySet();
    this.executor = executor;
    this.context = context;
    this.clock = clock;
  }

  public ReplicationSupervisor(
      ContainerSet containerSet, StateContext context,
      ContainerReplicator pullReplicator, ContainerReplicator pushReplicator,
      ReplicationConfig replicationConfig, Clock clock) {
    this(containerSet, context, pullReplicator, pushReplicator,
        new ThreadPoolExecutor(
            replicationConfig.getReplicationMaxStreams(),
            replicationConfig.getReplicationMaxStreams(), 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("ContainerReplicationThread-%d")
                .build()),
        clock);
  }

  /**
   * Queue an asynchronous download of the given container.
   */
  public void addTask(ReplicationTask task) {
    if (inFlight.add(task)) {
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
  public int getInFlightReplications() {
    return inFlight.size();
  }

  /**
   * An executable form of a replication task with status handling.
   */
  public final class TaskRunner implements Runnable {
    private final AbstractReplicationTask task;

    public TaskRunner(AbstractReplicationTask task) {
      this.task = task;
    }

    @Override
    public void run() {
      final Long containerId = task.getContainerId();
      try {
        requestCounter.incrementAndGet();

        if (task.getDeadline() > 0 && clock.millis() > task.getDeadline()) {
          LOG.info("Ignoring" +
              " {} since the current time {}ms is past the deadline {}ms",
              this, clock.millis(), task.getDeadline());
          timeoutCounter.incrementAndGet();
          return;
        }

        if (context != null) {
          DatanodeDetails dn = context.getParent().getDatanodeDetails();
          if (dn != null && dn.getPersistedOpState() !=
              HddsProtos.NodeOperationalState.IN_SERVICE) {
            LOG.info("Dn is of {} state. Ignore {}",
                dn.getPersistedOpState(), this);
            return;
          }

          final OptionalLong currentTerm = context.getTermOfLeaderSCM();
          final long taskTerm = task.getTerm();
          if (currentTerm.isPresent() && taskTerm < currentTerm.getAsLong()) {
            LOG.info("Ignoring {} since SCM leader has new term ({} < {})",
                this, taskTerm, currentTerm.getAsLong());
            return;
          }
        }

        // TODO - remove this along with the below block. Note we need to check
        //        if the target is "this node" and if so skip it.
       // final boolean pull = task.getTarget() == null;
        if (containerSet.getContainer(task.getContainerId()) != null) {
          LOG.debug("Container {} has already been downloaded.", containerId);
          return;
        }

        // TODO - remove this commented blocks and put replicator choice into
        //  the cmd
       // task.setStatus(Status.IN_PROGRESS);
       // ContainerReplicator replicator = pull ? pullReplicator : pushReplicator;
       // replicator.replicate(task);
        task.setStatus(Status.IN_PROGRESS);
        task.runTask();
        if (task.getStatus() == Status.FAILED) {
          LOG.error("Failed {}", this);
          failureCounter.incrementAndGet();
        } else if (task.getStatus() == Status.DONE) {
          LOG.info("Successful {}", this);
          successCounter.incrementAndGet();
        }
      } catch (Exception e) {
        task.setStatus(Status.FAILED);
        LOG.error("Failed {}", this, e);
        failureCounter.incrementAndGet();
      } finally {
        inFlight.remove(task);
      }
    }

    @Override
    public String toString() {
      return task.toString();
    }
  }

  public long getReplicationRequestCount() {
    return requestCounter.get();
  }

  public long getQueueSize() {
    if (executor instanceof ThreadPoolExecutor) {
      return ((ThreadPoolExecutor)executor).getQueue().size();
    } else {
      return 0;
    }
  }

  public long getReplicationSuccessCount() {
    return successCounter.get();
  }

  public long getReplicationFailureCount() {
    return failureCounter.get();
  }

  public long getReplicationTimeoutCount() {
    return timeoutCounter.get();
  }

}
