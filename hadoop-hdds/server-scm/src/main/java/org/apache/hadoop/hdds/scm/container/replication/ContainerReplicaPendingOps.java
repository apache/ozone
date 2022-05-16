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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.ADD;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.DELETE;

/**
 * Class to track pending replication operations across the cluster. For
 * each container with a pending replication or pending delete, there will
 * be an entry in this class mapping ContainerInfo to a list of the pending
 * operations.
 */
public class ContainerReplicaPendingOps {

  private final ConfigurationSource config;
  private final Clock clock;
  private final ConcurrentHashMap<ContainerInfo, List<ContainerReplicaOp>>
      pendingOps = new ConcurrentHashMap<>();

  public ContainerReplicaPendingOps(final ConfigurationSource conf,
      Clock clock) {
    this.config = conf;
    this.clock = clock;
  }

  /**
   * Get all the ContainerReplicaOp's associated with the given ContainerInfo.
   * A new list is created and returned, so it can be modified by the caller,
   * but any changes will not be reflected in the internal map.
   * @param containerInfo The containerInfo for which to retrieve the pending
   *                      ops.
   * @return Standalone list of ContainerReplica or an empty list if none exist.
   */
  public List<ContainerReplicaOp> getPendingOps(ContainerInfo containerInfo) {
    synchronized (containerInfo) {
      List<ContainerReplicaOp> ops = pendingOps.get(containerInfo);
      if (ops == null) {
        return Collections.emptyList();
      }
      return new ArrayList<>(ops);
    }
  }

  /**
   * Store a ContainerReplicaOp to add a replica for the given containerInfo.
   * @param containerInfo ContainerInfo for which to add a replica
   * @param target The target datanode
   * @param replicaIndex The replica index (zero for Ratis, > 0 for EC)
   */
  public void scheduleAddReplica(ContainerInfo containerInfo,
      DatanodeDetails target, int replicaIndex) {
    addReplica(ADD, containerInfo, target, replicaIndex);
  }

  /**
   * Store a ContainerReplicaOp to delete a replica for the given containerInfo.
   * @param containerInfo ContainerInfo for which to delete a replica
   * @param target The target datanode
   * @param replicaIndex The replica index (zero for Ratis, > 0 for EC)
   */
  public void scheduleDeleteReplica(ContainerInfo containerInfo,
      DatanodeDetails target, int replicaIndex) {
    addReplica(DELETE, containerInfo, target, replicaIndex);
  }

  /**
   * Remove a stored ContainerReplicaOp from the given containerInfo as it has
   * been replicated successfully.
   * @param containerInfo ContainerInfo for which to complete the replication
   * @param target The target Datanode
   * @param replicaIndex The replica index (zero for Ratis, > 0 for EC)
   * @return True if a pending replica was found and removed, false otherwise.
   */
  public boolean completeAddReplica(ContainerInfo containerInfo,
      DatanodeDetails target, int replicaIndex) {
    return completeOp(ADD, containerInfo, target, replicaIndex);
  }


  /**
   * Remove a stored ContainerReplicaOp from the given containerInfo as it has
   * been deleted successfully.
   * @param containerInfo ContainerInfo for which to complete the deletion
   * @param target The target Datanode
   * @param replicaIndex The replica index (zero for Ratis, > 0 for EC)
   * @return True if a pending replica was found and removed, false otherwise.
   */
  public boolean completeDeleteReplica(ContainerInfo containerInfo,
      DatanodeDetails target, int replicaIndex) {
    return completeOp(DELETE, containerInfo, target, replicaIndex);
  }

  /**
   * Iterate over all pending entries and remove any which have expired, meaning
   * they have not completed the operation inside the given time.
   * @param expiryMilliSeconds
   */
  public void removeExpiredEntries(long expiryMilliSeconds) {
    for (ContainerInfo containerInfo : pendingOps.keySet()) {
      // Rather than use an entry set, we get the map entry again. This is
      // to protect against another thread modifying the value after this
      // iterator started. Once we lock on the containerInfo object, no other
      // changes can occur to the list of ops associated with it.
      synchronized (containerInfo) {
        List<ContainerReplicaOp> ops = pendingOps.get(containerInfo);
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
          }
        }
        if (ops.size() == 0) {
          pendingOps.remove(containerInfo);
        }
      }
    }
  }

  private void addReplica(ContainerReplicaOp.PendingOpType opType,
      ContainerInfo containerInfo, DatanodeDetails target, int replicaIndex) {
    synchronized (containerInfo) {
      List<ContainerReplicaOp> ops = pendingOps.computeIfAbsent(
          containerInfo, s -> new ArrayList<>());
      ops.add(new ContainerReplicaOp(opType,
          target, replicaIndex, clock.millis()));
    }
  }

  private boolean completeOp(ContainerReplicaOp.PendingOpType opType,
      ContainerInfo containerInfo, DatanodeDetails target, int replicaIndex) {
    boolean found = false;
    synchronized (containerInfo) {
      List<ContainerReplicaOp> ops = pendingOps.get(containerInfo);
      if (ops != null) {
        Iterator<ContainerReplicaOp> iterator = ops.listIterator();
        while (iterator.hasNext()) {
          ContainerReplicaOp op = iterator.next();
          if (op.getOpType() == opType
              && op.getTarget().equals(target)
              && op.getReplicaIndex() == replicaIndex) {
            found = true;
            iterator.remove();
          }
        }
        if (ops.size() == 0) {
          pendingOps.remove(containerInfo);
        }
      }
    }
    return found;
  }

}
