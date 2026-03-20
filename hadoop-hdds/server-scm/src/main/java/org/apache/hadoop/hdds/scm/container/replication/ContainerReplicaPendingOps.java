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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.ADD;
import static org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaOp.PendingOpType.DELETE;

/**
 * Class to track pending replications for a container.
 */
public class ContainerReplicaPendingOps {

  private final Map<ContainerID, List<ContainerReplicaOp>> pendingOps;
  private final List<ContainerReplicaPendingOpsSubscriber> subscribers;
  private ReplicationManagerMetrics replicationMetrics;
  private final java.time.Clock clock;

  public ContainerReplicaPendingOps(java.time.Clock clock) {
    this.clock = clock;
    pendingOps = new ConcurrentHashMap<>();
    subscribers = new CopyOnWriteArrayList<>();
  }

  public void setReplicationMetrics(ReplicationManagerMetrics metrics) {
    this.replicationMetrics = metrics;
  }

  public void registerSubscriber(ContainerReplicaPendingOpsSubscriber subscriber) {
    subscribers.add(subscriber);
  }

  public void unregisterSubscriber(ContainerReplicaPendingOpsSubscriber subscriber) {
    subscribers.remove(subscriber);
  }

  public List<ContainerReplicaOp> getPendingOps(ContainerID containerID) {
    List<ContainerReplicaOp> ops = pendingOps.get(containerID);
    return ops == null ? Collections.emptyList() : Collections.unmodifiableList(ops);
  }

  public void scheduleAddReplica(ContainerID containerID, DatanodeDetails target,
      int replicaIndex, SCMCommand<?> command, long scmDeadlineEpochMs,
      long containerSize, long scheduledEpochMs, DatanodeDetails decommissionSource) {
    addReplica(ADD, containerID, target, replicaIndex, command,
        scmDeadlineEpochMs, containerSize, decommissionSource);
  }

  public void scheduleAddReplica(ContainerID containerID, DatanodeDetails target,
      int replicaIndex, long scmDeadlineEpochMs) {
    scheduleAddReplica(containerID, target, replicaIndex, null,
        scmDeadlineEpochMs, 0, Time.now(), null);
  }

  public void scheduleDeleteReplica(ContainerID containerID, DatanodeDetails target,
      int replicaIndex, long scmDeadlineEpochMs) {
    scheduleDeleteReplica(containerID, target, replicaIndex, null,
        scmDeadlineEpochMs, null);
  }

  public void scheduleDeleteReplica(ContainerID containerID, DatanodeDetails target,
      int replicaIndex, SCMCommand<?> command, long scmDeadlineEpochMs,
      DatanodeDetails decommissionSource) {
    addReplica(DELETE, containerID, target, replicaIndex, command,
        scmDeadlineEpochMs, 0, decommissionSource);
  }

  private void addReplica(ContainerReplicaOp.PendingOpType opType,
      ContainerID containerID, DatanodeDetails target, int replicaIndex,
      SCMCommand<?> command, long scmDeadlineEpochMs, long containerSize,
      DatanodeDetails decommissionSource) {
    pendingOps.computeIfAbsent(containerID, k -> new CopyOnWriteArrayList<>())
        .add(new ContainerReplicaOp(opType, target, replicaIndex, command,
            scmDeadlineEpochMs, containerSize, decommissionSource));
  }

  public boolean completeAddReplica(ContainerID containerID, DatanodeDetails target,
      int replicaIndex) {
    return completeOp(ADD, containerID, target, replicaIndex);
  }

  public boolean completeDeleteReplica(ContainerID containerID, DatanodeDetails target,
      int replicaIndex) {
    return completeOp(DELETE, containerID, target, replicaIndex);
  }

  private boolean completeOp(ContainerReplicaOp.PendingOpType opType,
      ContainerID containerID, DatanodeDetails target, int replicaIndex) {
    List<ContainerReplicaOp> ops = pendingOps.get(containerID);
    if (ops != null) {
      ContainerReplicaOp foundOp = null;
      for (ContainerReplicaOp op : ops) {
        if (op.getOpType() == opType && op.getTarget().equals(target)
            && op.getReplicaIndex() == replicaIndex) {
          foundOp = op;
          break;
        }
      }
      if (foundOp != null) {
        ops.remove(foundOp);
        notifySubscribers(foundOp, containerID, false);
        if (ops.isEmpty()) {
          pendingOps.remove(containerID);
        }
        return true;
      }
    }
    return false;
  }

  public void removeExpiredOps(long scmDeadlineEpochMs) {
    for (Map.Entry<ContainerID, List<ContainerReplicaOp>> entry : pendingOps.entrySet()) {
      List<ContainerReplicaOp> ops = entry.getValue();
      List<ContainerReplicaOp> expiredOps = new ArrayList<>();
      for (ContainerReplicaOp op : ops) {
        if (op.getDeadlineEpochMillis() < scmDeadlineEpochMs) {
          expiredOps.add(op);
        }
      }
      for (ContainerReplicaOp op : expiredOps) {
        ops.remove(op);
        notifySubscribers(op, entry.getKey(), true);
      }
      if (ops.isEmpty()) {
        pendingOps.remove(entry.getKey());
      }
    }
  }

  private void notifySubscribers(ContainerReplicaOp op, ContainerID containerID,
      boolean timedOut) {
    for (ContainerReplicaPendingOpsSubscriber subscriber : subscribers) {
      subscriber.opCompleted(op, containerID, timedOut);
    }
  }

  public long getPendingOpCount(ContainerReplicaOp.PendingOpType opType,
      HddsProtos.ReplicationType replicationType) {
    long count = 0;
    for (List<ContainerReplicaOp> ops : pendingOps.values()) {
      for (ContainerReplicaOp op : ops) {
        if (op.getOpType() == opType) {
          if (replicationType == HddsProtos.ReplicationType.EC && op.getReplicaIndex() > 0) {
            count++;
          } else if (replicationType == HddsProtos.ReplicationType.RATIS && op.getReplicaIndex() == 0) {
            count++;
          }
        }
      }
    }
    return count;
  }

  public long getPendingOpCount(ContainerReplicaOp.PendingOpType opType) {
    long count = 0;
    for (List<ContainerReplicaOp> ops : pendingOps.values()) {
      for (ContainerReplicaOp op : ops) {
        if (op.getOpType() == opType) {
          count++;
        }
      }
    }
    return count;
  }

  public void clear() {
    pendingOps.clear();
  }
}
