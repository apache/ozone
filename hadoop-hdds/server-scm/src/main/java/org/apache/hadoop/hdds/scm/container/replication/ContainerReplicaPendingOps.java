/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.util.concurrent.Striped;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.ADD;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.DELETE;

/**
 * Class to track pending replication operations across the cluster. For
 * each container with a pending replication or pending delete, there will
 * be an entry in this class mapping ContainerID to a list of the pending
 * operations.
 */
public class ContainerReplicaPendingOps {

  private final ConfigurationSource config;
  private final Clock clock;
  private final ConcurrentHashMap<ContainerID, List<ContainerReplicaOp>>
      pendingOps = new ConcurrentHashMap<>();
  private final Striped<ReadWriteLock> stripedLock = Striped.readWriteLock(64);
  private final ConcurrentHashMap<PendingOpType, AtomicLong>
      pendingOpCount = new ConcurrentHashMap<>();
  private ReplicationManagerMetrics replicationMetrics = null;

  public ContainerReplicaPendingOps(final ConfigurationSource conf,
      Clock clock) {
    this.config = conf;
    this.clock = clock;
    for (PendingOpType opType: PendingOpType.values()) {
      pendingOpCount.put(opType, new AtomicLong(0));
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
    lock.lock();
    try {
      List<ContainerReplicaOp> ops = pendingOps.get(containerID);
      if (ops == null) {
        return Collections.emptyList();
      }
      return new ArrayList<>(ops);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Store a ContainerReplicaOp to add a replica for the given ContainerID.
   * @param containerID ContainerID for which to add a replica
   * @param target The target datanode
   * @param replicaIndex The replica index (zero for Ratis, > 0 for EC)
   */
  public void scheduleAddReplica(ContainerID containerID,
      DatanodeDetails target, int replicaIndex) {
    addReplica(ADD, containerID, target, replicaIndex);
  }

  /**
   * Store a ContainerReplicaOp to delete a replica for the given ContainerID.
   * @param containerID ContainerID for which to delete a replica
   * @param target The target datanode
   * @param replicaIndex The replica index (zero for Ratis, > 0 for EC)
   */
  public void scheduleDeleteReplica(ContainerID containerID,
      DatanodeDetails target, int replicaIndex) {
    addReplica(DELETE, containerID, target, replicaIndex);
  }

  /**
   * Remove a stored ContainerReplicaOp from the given ContainerID as it has
   * been replicated successfully.
   * @param containerID ContainerID for which to complete the replication
   * @param target The target Datanode
   * @param replicaIndex The replica index (zero for Ratis, > 0 for EC)
   * @return True if a pending replica was found and removed, false otherwise.
   */
  public boolean completeAddReplica(ContainerID containerID,
      DatanodeDetails target, int replicaIndex) {
    boolean completed = completeOp(ADD, containerID, target, replicaIndex);
    if (isMetricsNotNull() && completed) {
      if (replicaIndex > 0) {
        replicationMetrics.incrEcReplicationCmdsCompletedTotal();
      } else if (replicaIndex == 0) {
        replicationMetrics.incrNumReplicationCmdsCompleted();
      }
    }
    return completed;
  }


  /**
   * Remove a stored ContainerReplicaOp from the given ContainerID as it has
   * been deleted successfully.
   * @param containerID ContainerID for which to complete the deletion
   * @param target The target Datanode
   * @param replicaIndex The replica index (zero for Ratis, > 0 for EC)
   * @return True if a pending replica was found and removed, false otherwise.
   */
  public boolean completeDeleteReplica(ContainerID containerID,
      DatanodeDetails target, int replicaIndex) {
    boolean completed = completeOp(DELETE, containerID, target, replicaIndex);
    if (isMetricsNotNull() && completed) {
      if (replicaIndex > 0) {
        replicationMetrics.incrEcDeletionCmdsCompletedTotal();
      } else if (replicaIndex == 0) {
        replicationMetrics.incrNumDeletionCmdsCompleted();
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
        op.getReplicaIndex());
  }

  /**
   * Iterate over all pending entries and remove any which have expired, meaning
   * they have not completed the operation inside the given time.
   * @param expiryMilliSeconds
   */
  public void removeExpiredEntries(long expiryMilliSeconds) {
    for (ContainerID containerID : pendingOps.keySet()) {
      // Rather than use an entry set, we get the map entry again. This is
      // to protect against another thread modifying the value after this
      // iterator started. Once we lock on the ContainerID object, no other
      // changes can occur to the list of ops associated with it.
      Lock lock = writeLock(containerID);
      lock.lock();
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
          if (op.getScheduledEpochMillis() + expiryMilliSeconds
              < clock.millis()) {
            iterator.remove();
            pendingOpCount.get(op.getOpType()).decrementAndGet();
            updateTimeoutMetrics(op);
          }
        }
        if (ops.size() == 0) {
          pendingOps.remove(containerID);
        }
      } finally {
        lock.unlock();
      }
    }
  }

  private void updateTimeoutMetrics(ContainerReplicaOp op) {
    if (op.getOpType() == ADD && isMetricsNotNull()) {
      if (op.getReplicaIndex() > 0) {
        replicationMetrics.incrEcReplicationCmdsTimeoutTotal();
      } else if (op.getReplicaIndex() == 0) {
        replicationMetrics.incrNumReplicationCmdsTimeout();
      }
    } else if (op.getOpType() == DELETE && isMetricsNotNull()) {
      if (op.getReplicaIndex() > 0) {
        replicationMetrics.incrEcDeletionCmdsTimeoutTotal();
      } else if (op.getReplicaIndex() == 0) {
        replicationMetrics.incrNumDeletionCmdsTimeout();
      }
    }
  }

  private void addReplica(ContainerReplicaOp.PendingOpType opType,
      ContainerID containerID, DatanodeDetails target, int replicaIndex) {
    Lock lock = writeLock(containerID);
    lock.lock();
    try {
      List<ContainerReplicaOp> ops = pendingOps.computeIfAbsent(
          containerID, s -> new ArrayList<>());
      ops.add(new ContainerReplicaOp(opType,
          target, replicaIndex, clock.millis()));
      pendingOpCount.get(opType).incrementAndGet();
    } finally {
      lock.unlock();
    }
  }

  private boolean completeOp(ContainerReplicaOp.PendingOpType opType,
      ContainerID containerID, DatanodeDetails target, int replicaIndex) {
    boolean found = false;
    Lock lock = writeLock(containerID);
    lock.lock();
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
            iterator.remove();
            pendingOpCount.get(op.getOpType()).decrementAndGet();
          }
        }
        if (ops.size() == 0) {
          pendingOps.remove(containerID);
        }
      }
    } finally {
      lock.unlock();
    }
    return found;
  }

  private Lock writeLock(ContainerID containerID) {
    return stripedLock.get(containerID).writeLock();
  }

  private Lock readLock(ContainerID containerID) {
    return stripedLock.get(containerID).readLock();
  }

  private boolean isMetricsNotNull() {
    return replicationMetrics != null;
  }

  public void setReplicationMetrics(
      ReplicationManagerMetrics replicationMetrics) {
    this.replicationMetrics = replicationMetrics;
  }

  public long getPendingOpCount(PendingOpType opType) {
    return pendingOpCount.get(opType).get();
  }
}
