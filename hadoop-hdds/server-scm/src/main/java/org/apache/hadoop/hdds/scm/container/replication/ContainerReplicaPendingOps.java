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

package org.apache.hadoop.hdds.scm.container.replication;

import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.ADD;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.DELETE;

import com.google.common.util.concurrent.Striped;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

/**
 * Class to track pending replication operations across the cluster. For
 * each container with a pending replication or pending delete, there will
 * be an entry in this class mapping ContainerID to a list of the pending
 * operations.
 */
public class ContainerReplicaPendingOps {

  private static final int RATIS_COUNTER_INDEX = 0;
  private static final int EC_COUNTER_INDEX = 1;

  private final Clock clock;
  private final ConcurrentHashMap<ContainerID, List<ContainerReplicaOp>>
      pendingOps = new ConcurrentHashMap<>();
  private final Striped<ReadWriteLock> stripedLock = Striped.readWriteLock(64);
  private final ReentrantReadWriteLock globalLock =
      new ReentrantReadWriteLock();
  private final ConcurrentHashMap<PendingOpType, AtomicLong[]>
      pendingOpCount = new ConcurrentHashMap<>();
  private ReplicationManagerMetrics replicationMetrics = null;
  private final List<ContainerReplicaPendingOpsSubscriber> subscribers =
      new ArrayList<>();
  // tracks how much data is pending to be added to a target Datanode because of pending ADD ops
  private final ConcurrentHashMap<DatanodeID, SizeAndTime> containerSizeScheduled = new ConcurrentHashMap<>();
  private ReplicationManager.ReplicationManagerConfiguration rmConf;

  /**
   * Creates a ContainerReplicaPendingOps with all parameters.
   * This is the single constructor that should be used for all cases.
   *
   * @param clock the clock to use for timing operations
   * @param rmConf the replication manager configuration (can be null)
   */
  public ContainerReplicaPendingOps(Clock clock, ReplicationManager.ReplicationManagerConfiguration rmConf) {
    this.clock = clock;
    this.rmConf = rmConf;
    resetCounters();
  }

  /**
   * Used as the value of {@link ContainerReplicaPendingOps#containerSizeScheduled} map for tracking the size of
   * containers with pending ADD ops. Immutable.
   */
  public static class SizeAndTime {
    // number of bytes pending ADD on a target DN
    private final long size;
    // timestamp (milliseconds since epoch) when the latest op was scheduled for this DN
    private final long lastUpdatedTime;

    public SizeAndTime(long size, long lastUpdatedTime) {
      this.size = size;
      this.lastUpdatedTime = lastUpdatedTime;
    }

    public long getSize() {
      return size;
    }

    public long getLastUpdatedTime() {
      return lastUpdatedTime;
    }

    @Override
    public String toString() {
      return "Size: " + size + ", lastUpdatedTime: " + lastUpdatedTime;
    }
  }

  private void resetCounters() {
    for (PendingOpType opType: PendingOpType.values()) {
      AtomicLong[] counters = new AtomicLong[2];
      counters[RATIS_COUNTER_INDEX] = new AtomicLong(0);
      counters[EC_COUNTER_INDEX] = new AtomicLong(0);
      pendingOpCount.put(opType, counters);
    }
  }

  /**
   * Clears all pendingOps and resets all the counters to zero.
   */
  public void clear() {
    // We block all other concurrent access with the global lock to prevent
    // the map and counters getting out of sync if there are concurrent updates
    // happening when the class is cleared.
    globalLock.writeLock().lock();
    try {
      pendingOps.clear();
      resetCounters();
      containerSizeScheduled.clear();
    } finally {
      globalLock.writeLock().unlock();
    }
  }

  /**
   * Get all the ContainerReplicaOp's associated with the given ContainerID.
   * A new list is created and returned, so it can be modified by the caller,
   * but any changes will not be reflected in the internal map.
   * @param containerID The ContainerID for which to retrieve the pending
   *                      ops.
   * @return Standalone list of ContainerReplica or an empty list if none exist.
   */
  public List<ContainerReplicaOp> getPendingOps(ContainerID containerID) {
    Lock lock = readLock(containerID);
    lock(lock);
    try {
      List<ContainerReplicaOp> ops = pendingOps.get(containerID);
      if (ops == null) {
        return Collections.emptyList();
      }
      return new ArrayList<>(ops);
    } finally {
      unlock(lock);
    }
  }

  /**
   * Store a ContainerReplicaOp to add a replica for the given ContainerID.
   * @param containerID ContainerID for which to add a replica
   * @param target The target datanode
   * @param replicaIndex The replica index (zero for Ratis, &gt; 0 for EC)
   * @param command The command to send to the datanode
   * @param deadlineEpochMillis The time by which the replica should have been
   *                            added and reported by the datanode, or it will
   *                            be discarded.
   */
  public void scheduleAddReplica(ContainerID containerID,
      DatanodeDetails target, int replicaIndex, SCMCommand<?> command, long deadlineEpochMillis, long containerSize,
      long scheduledEpochMillis) {
    addReplica(ADD, containerID, target, replicaIndex, command, deadlineEpochMillis, containerSize,
        scheduledEpochMillis);
  }

  /**
   * Store a ContainerReplicaOp to delete a replica for the given ContainerID.
   * @param containerID ContainerID for which to delete a replica
   * @param target The target datanode
   * @param replicaIndex The replica index (zero for Ratis, &gt; 0 for EC)
   * @param command The command to send to the datanode
   * @param deadlineEpochMillis The time by which the replica should have been
   *                            deleted and reported by the datanode, or it will
   *                            be discarded.
   */
  public void scheduleDeleteReplica(ContainerID containerID,
      DatanodeDetails target, int replicaIndex, SCMCommand<?> command, long deadlineEpochMillis) {
    addReplica(DELETE, containerID, target, replicaIndex, command, deadlineEpochMillis, 0L, clock.millis());
  }

  /**
   * Remove a stored ContainerReplicaOp from the given ContainerID as it has
   * been replicated successfully.
   * @param containerID ContainerID for which to complete the replication
   * @param target The target Datanode
   * @param replicaIndex The replica index (zero for Ratis, &gt; 0 for EC)
   * @return True if a pending replica was found and removed, false otherwise.
   */
  public boolean completeAddReplica(ContainerID containerID,
      DatanodeDetails target, int replicaIndex) {
    boolean completed = completeOp(ADD, containerID, target, replicaIndex, true);
    if (isMetricsNotNull() && completed) {
      if (isEC(replicaIndex)) {
        replicationMetrics.incrEcReplicasCreatedTotal();
      } else {
        replicationMetrics.incrReplicasCreatedTotal();
      }
    }
    return completed;
  }

  /**
   * Remove a stored ContainerReplicaOp from the given ContainerID as it has
   * been deleted successfully.
   * @param containerID ContainerID for which to complete the deletion
   * @param target The target Datanode
   * @param replicaIndex The replica index (zero for Ratis, &gt; 0 for EC)
   * @return True if a pending replica was found and removed, false otherwise.
   */
  public boolean completeDeleteReplica(ContainerID containerID,
      DatanodeDetails target, int replicaIndex) {
    boolean completed = completeOp(DELETE, containerID, target, replicaIndex, true);
    if (isMetricsNotNull() && completed) {
      if (isEC(replicaIndex)) {
        replicationMetrics.incrEcReplicasDeletedTotal();
      } else {
        replicationMetrics.incrReplicasDeletedTotal();
      }
    }
    return completed;
  }

  /**
   * Remove a stored pending operation from the given ContainerID.
   * @param containerID ContainerID for which to remove the op.
   * @param op ContainerReplicaOp to remove
   * @return True if an element was found and deleted, false otherwise.
   */
  public boolean removeOp(ContainerID containerID,
      ContainerReplicaOp op) {
    return completeOp(op.getOpType(), containerID, op.getTarget(),
        op.getReplicaIndex(), true);
  }

  /**
   * Iterate over all pending entries and remove any which have expired, meaning
   * they have not completed the operation inside the given time.
   */
  public void removeExpiredEntries() {
    for (ContainerID containerID : pendingOps.keySet()) {
      // List of expired ops that subscribers will be notified about
      List<ContainerReplicaOp> expiredOps = new ArrayList<>();

      // Rather than use an entry set, we get the map entry again. This is
      // to protect against another thread modifying the value after this
      // iterator started. Once we lock on the ContainerID object, no other
      // changes can occur to the list of ops associated with it.
      Lock lock = writeLock(containerID);
      lock(lock);
      try {
        List<ContainerReplicaOp> ops = pendingOps.get(containerID);
        if (ops == null) {
          // There should not be null entries, but another thread may have
          // removed the map entry after the iterator was started.
          continue;
        }
        Iterator<ContainerReplicaOp> iterator = ops.listIterator();
        while (iterator.hasNext()) {
          ContainerReplicaOp op = iterator.next();
          if (clock.millis() > op.getDeadlineEpochMillis()) {
            if (op.getOpType() != DELETE) {
              // For delete ops, we don't remove them from the list as RM must resend them, or they
              // will be removed via a container report when they are confirmed as deleted.
              iterator.remove();
              if (op.getOpType() == ADD) {
                releaseScheduledContainerSize(op);
              }
              decrementCounter(op.getOpType(), op.getReplicaIndex());
            }
            expiredOps.add(op);
            updateTimeoutMetrics(op);
          }
        }
        if (ops.isEmpty()) {
          pendingOps.remove(containerID);
        }
      } finally {
        unlock(lock);
      }

      // notify if there are expired ops
      if (!expiredOps.isEmpty()) {
        notifySubscribers(expiredOps, containerID, true);
      }
    }
  }

  private void releaseScheduledContainerSize(ContainerReplicaOp op) {
    containerSizeScheduled.computeIfPresent(op.getTarget().getID(), (k, v) -> {
      long newSize = v.getSize() - op.getContainerSize();
      boolean isSizeNonPositive = newSize <= 0;
      boolean hasOpExpired = clock.millis() - v.getLastUpdatedTime() > rmConf.getEventTimeout();
      if (isSizeNonPositive || hasOpExpired) {
        /*
        If the scheduled size is now less than or equal to 0, or if the last op has expired, implying that the ops
        before it must have completed or expired, then remove this entry from the map
        */
        return null;
      }
      return new SizeAndTime(newSize, v.getLastUpdatedTime());
    });
  }

  private void updateTimeoutMetrics(ContainerReplicaOp op) {
    if (op.getOpType() == ADD && isMetricsNotNull()) {
      if (isEC(op.getReplicaIndex())) {
        replicationMetrics.incrEcReplicaCreateTimeoutTotal();
      } else {
        replicationMetrics.incrReplicaCreateTimeoutTotal();
      }
    } else if (op.getOpType() == DELETE && isMetricsNotNull()) {
      if (isEC(op.getReplicaIndex())) {
        replicationMetrics.incrEcReplicaDeleteTimeoutTotal();
      } else {
        replicationMetrics.incrReplicaDeleteTimeoutTotal();
      }
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private void addReplica(ContainerReplicaOp.PendingOpType opType,
      ContainerID containerID, DatanodeDetails target, int replicaIndex, SCMCommand<?> command,
      long deadlineEpochMillis, long containerSize, long scheduledEpochMillis) {
    Lock lock = writeLock(containerID);
    lock(lock);
    try {
      // Remove any existing duplicate op for the same target and replicaIndex before adding
      // the new one. Especially for delete ops, they could be getting resent after expiry.
      completeOp(opType, containerID, target, replicaIndex, false);
      List<ContainerReplicaOp> ops = pendingOps.computeIfAbsent(
          containerID, s -> new ArrayList<>());
      ops.add(new ContainerReplicaOp(opType,
          target, replicaIndex, command, deadlineEpochMillis, containerSize));
      DatanodeID id = target.getID();
      if (opType == ADD) {
        containerSizeScheduled.compute(id, (k, v) -> {
          if (v == null) {
            return new SizeAndTime(containerSize, scheduledEpochMillis);
          } else {
            return new SizeAndTime(v.getSize() + containerSize, scheduledEpochMillis);
          }
        });
      }
      incrementCounter(opType, replicaIndex);
    } finally {
      unlock(lock);
    }
  }

  private boolean completeOp(ContainerReplicaOp.PendingOpType opType,
      ContainerID containerID, DatanodeDetails target, int replicaIndex, boolean notifySubsribers) {
    boolean found = false;
    // List of completed ops that subscribers will be notified about
    List<ContainerReplicaOp> completedOps = new ArrayList<>();
    Lock lock = writeLock(containerID);
    lock(lock);
    try {
      List<ContainerReplicaOp> ops = pendingOps.get(containerID);
      if (ops != null) {
        Iterator<ContainerReplicaOp> iterator = ops.listIterator();
        while (iterator.hasNext()) {
          ContainerReplicaOp op = iterator.next();
          if (op.getOpType() == opType
              && op.getTarget().equals(target)
              && op.getReplicaIndex() == replicaIndex) {
            found = true;
            completedOps.add(op);
            iterator.remove();
            if (opType == ADD) {
              containerSizeScheduled.computeIfPresent(target.getID(), (k, v) -> {
                long newSize = v.getSize() - op.getContainerSize();
                if (newSize <= 0) {
                  return null;
                }
                return new SizeAndTime(newSize, v.getLastUpdatedTime());
              });
            }
            decrementCounter(op.getOpType(), replicaIndex);
          }
        }
        if (ops.isEmpty()) {
          pendingOps.remove(containerID);
        }
      }
    } finally {
      unlock(lock);
    }

    if (found && notifySubsribers) {
      notifySubscribers(completedOps, containerID, false);
    }
    return found;
  }

  /**
   * Notifies subscribers about the specified ops by calling
   * ContainerReplicaPendingOpsSubscriber#opCompleted.
   *
   * @param ops the ops to send notifications for
   * @param containerID the container that ops belong to
   * @param timedOut true if the ops (each one) expired, false if they completed
   */
  private void notifySubscribers(List<ContainerReplicaOp> ops,
      ContainerID containerID, boolean timedOut) {
    for (ContainerReplicaOp op : ops) {
      for (ContainerReplicaPendingOpsSubscriber subscriber : subscribers) {
        subscriber.opCompleted(op, containerID, timedOut);
      }
    }
  }

  /**
   * Registers a subscriber that will be notified about completed ops.
   *
   * @param subscriber object that wants to subscribe
   */
  public void registerSubscriber(
      ContainerReplicaPendingOpsSubscriber subscriber) {
    subscribers.add(subscriber);
  }

  private Lock writeLock(ContainerID containerID) {
    return stripedLock.get(containerID).writeLock();
  }

  private Lock readLock(ContainerID containerID) {
    return stripedLock.get(containerID).readLock();
  }

  private void lock(Lock lock) {
    // We always take the global lock in shared / read mode as the only time it
    // will block is when the class is getting cleared to remove all operations.
    globalLock.readLock().lock();
    lock.lock();
  }

  private void unlock(Lock lock) {
    globalLock.readLock().unlock();
    lock.unlock();
  }

  public ConcurrentHashMap<DatanodeID, SizeAndTime> getContainerSizeScheduled() {
    return containerSizeScheduled;
  }

  public Clock getClock() {
    return clock;
  }

  private boolean isMetricsNotNull() {
    return replicationMetrics != null;
  }

  public void setReplicationMetrics(
      ReplicationManagerMetrics replicationMetrics) {
    this.replicationMetrics = replicationMetrics;
  }

  public long getPendingOpCount(PendingOpType opType) {
    AtomicLong[] counters = pendingOpCount.get(opType);
    long count = 0;
    for (AtomicLong counter : counters) {
      count += counter.get();
    }
    return count;
  }

  public long getPendingOpCount(PendingOpType opType, ReplicationType type) {
    int index = RATIS_COUNTER_INDEX;
    if (type == ReplicationType.EC) {
      index = EC_COUNTER_INDEX;
    }
    return pendingOpCount.get(opType)[index].get();
  }

  private long incrementCounter(PendingOpType type, int replicaIndex) {
    return pendingOpCount.get(type)[counterIndex(replicaIndex)]
        .incrementAndGet();
  }

  private long decrementCounter(PendingOpType type, int replicaIndex) {
    return pendingOpCount.get(type)[counterIndex(replicaIndex)]
        .decrementAndGet();
  }

  private int counterIndex(int replicaIndex) {
    if (isEC(replicaIndex)) {
      return EC_COUNTER_INDEX;
    } else {
      return RATIS_COUNTER_INDEX;
    }
  }

  private boolean isEC(int replicaIndex) {
    return replicaIndex > 0;
  }

}
