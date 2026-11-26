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

package org.apache.hadoop.hdds.scm.container.balancer;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStorageTypeStatusInfoProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.NodeTransferInfoProto;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.Interns;

/**
 * Results of Container Balancer, kept separately for each storage type. Every series carries a
 * {@code StorageType} tag; a move whose type is unknown is reported as {@value #UNKNOWN}.
 * <p>
 * A move is counted under the storage type and the iteration that scheduled it. Each iteration has a generation, and
 * a result is accepted only while that generation is still open. A result that arrives after its iteration has
 * finished is dropped, so it can neither change the finished iteration nor the totals of a later one.
 */
public final class ContainerBalancerStorageTypeMetrics implements MetricsSource {
  public static final String NAME = ContainerBalancerStorageTypeMetrics.class.getSimpleName();
  public static final String UNKNOWN = "UNKNOWN";

  private static final MetricsInfo STORAGE_TYPE = Interns.info("StorageType", "Storage type of the balanced replicas");

  private final ConcurrentMap<String, TypeMetrics> byType = new ConcurrentHashMap<>();

  /** Returns the metrics of a storage type; {@code null} is the bucket for moves of unknown type. */
  TypeMetrics forType(StorageType type) {
    return byType.computeIfAbsent(label(type), ignored -> new TypeMetrics(type));
  }

  /** Starts a new iteration: forgets the latest results of every type and opens a new generation. */
  void startIteration() {
    byType.values().forEach(TypeMetrics::startIteration);
  }

  /** Ends the iteration: late results are dropped from now on, and the latest results join the totals. */
  void finishIteration() {
    byType.values().forEach(TypeMetrics::finishIteration);
  }

  /** Returns the results of the latest iteration of every type that took part in it. */
  List<ContainerBalancerStorageTypeStatusInfoProto> statusInfo() {
    return byType.values().stream()
        .map(TypeMetrics::toProto)
        .filter(Objects::nonNull)
        .sorted(Comparator.comparing(proto -> proto.hasStorageType() ? proto.getStorageType().name() : UNKNOWN))
        .collect(Collectors.toList());
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    byType.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(entry -> entry.getValue().snapshot(collector.addRecord(NAME).tag(STORAGE_TYPE, entry.getKey())));
  }

  private static String label(StorageType type) {
    return type == null ? UNKNOWN : type.name();
  }

  /** How a finished move is counted. */
  enum Outcome {
    COMPLETED,
    TIMEOUT,
    FAILED,
    /** Counted nowhere, for example a move that was cancelled. */
    NONE;

    /** The one place that decides how a {@link MoveManager.MoveResult} is counted, for every counter. */
    static Outcome of(MoveManager.MoveResult result) {
      if (result == null) {
        return NONE;
      }
      switch (result) {
      case COMPLETED:
        return COMPLETED;
      case REPLICATION_FAIL_TIME_OUT:
      case DELETION_FAIL_TIME_OUT:
        return TIMEOUT;
      case FAIL_LEADER_NOT_READY:
      case REPLICATION_FAIL_INFLIGHT_REPLICATION:
      case REPLICATION_FAIL_NOT_EXIST_IN_SOURCE:
      case REPLICATION_FAIL_STORAGE_TYPE_MISMATCH:
      case REPLICATION_FAIL_EXIST_IN_TARGET:
      case REPLICATION_FAIL_CONTAINER_NOT_CLOSED:
      case REPLICATION_FAIL_INFLIGHT_DELETION:
      case REPLICATION_FAIL_NODE_NOT_IN_SERVICE:
      case DELETION_FAIL_NODE_NOT_IN_SERVICE:
      case REPLICATION_FAIL_NODE_UNHEALTHY:
      case DELETION_FAIL_NODE_UNHEALTHY:
      case DELETE_FAIL_POLICY:
      case REPLICATION_NOT_HEALTHY_BEFORE_MOVE:
      case REPLICATION_NOT_HEALTHY_AFTER_MOVE:
      case FAIL_CONTAINER_ALREADY_BEING_MOVED:
      case FAIL_UNEXPECTED_ERROR:
        return FAILED;
      default:
        return NONE;
      }
    }
  }

  /** Results of one storage type. All state is guarded by the instance lock. */
  static final class TypeMetrics {
    private final StorageType type;

    // The latest iteration.
    private long generation;
    private boolean active;
    private boolean open;
    private long sizeScheduled;
    private long dataSizeMoved;
    private long scheduled;
    private long completed;
    private long failed;
    private long timeout;
    private long numDatanodesUnbalanced;
    private long dataSizeUnbalanced;
    private final Map<DatanodeID, Long> sizeEntering = new HashMap<>();
    private final Map<DatanodeID, Long> sizeLeaving = new HashMap<>();

    // All finished iterations since SCM started.
    private long totalScheduled;
    private long totalCompleted;
    private long totalFailed;
    private long totalTimeout;
    private long totalDataSizeMoved;

    private TypeMetrics(StorageType type) {
      this.type = type;
    }

    synchronized void startIteration() {
      generation++;
      active = false;
      open = false;
      sizeScheduled = 0;
      dataSizeMoved = 0;
      scheduled = 0;
      completed = 0;
      failed = 0;
      timeout = 0;
      numDatanodesUnbalanced = 0;
      dataSizeUnbalanced = 0;
      sizeEntering.clear();
      sizeLeaving.clear();
    }

    /** Marks the type as part of the current iteration and opens it for results. */
    synchronized void initialize(long unbalancedNodes, long unbalancedBytes) {
      active = true;
      open = true;
      numDatanodesUnbalanced = unbalancedNodes;
      dataSizeUnbalanced = unbalancedBytes;
    }

    synchronized void finishIteration() {
      if (!open) {
        return;
      }
      open = false;
      totalScheduled += scheduled;
      totalCompleted += completed;
      totalFailed += failed;
      totalTimeout += timeout;
      totalDataSizeMoved += dataSizeMoved;
    }

    /** The generation a move must present to have its result counted. */
    synchronized long generation() {
      return generation;
    }

    synchronized void recordScheduled() {
      if (open) {
        scheduled++;
      }
    }

    synchronized void recordTraffic(DatanodeID source, DatanodeID target, long bytes) {
      if (open) {
        sizeScheduled += bytes;
        sizeLeaving.merge(source, bytes, Long::sum);
        sizeEntering.merge(target, bytes, Long::sum);
      }
    }

    /**
     * Counts the result of a move.
     *
     * @param outcome how the move ended
     * @param bytes size of the container, used when the move completed
     * @param moveGeneration the generation that was current when the move was scheduled
     * @return true if the result was counted; false if it is dropped because the iteration of the move is over or
     *     the outcome is not counted
     */
    synchronized boolean recordResult(Outcome outcome, long bytes, long moveGeneration) {
      if (!open || moveGeneration != generation || outcome == Outcome.NONE) {
        return false;
      }
      switch (outcome) {
      case COMPLETED:
        completed++;
        dataSizeMoved += bytes;
        break;
      case TIMEOUT:
        timeout++;
        break;
      default:
        failed++;
        break;
      }
      return true;
    }

    private synchronized ContainerBalancerStorageTypeStatusInfoProto toProto() {
      if (!active) {
        return null;
      }
      Set<DatanodeID> involved = new HashSet<>(sizeEntering.keySet());
      involved.addAll(sizeLeaving.keySet());
      ContainerBalancerStorageTypeStatusInfoProto.Builder builder =
          ContainerBalancerStorageTypeStatusInfoProto.newBuilder()
              .setSizeScheduledForMove(sizeScheduled)
              .setDataSizeMoved(dataSizeMoved)
              .setContainerMovesScheduled(scheduled)
              .setContainerMovesCompleted(completed)
              .setContainerMovesFailed(failed)
              .setContainerMovesTimeout(timeout)
              .setNumDatanodesInvolved(involved.size())
              .setNumDatanodesUnbalanced(numDatanodesUnbalanced)
              .setDataSizeUnbalanced(dataSizeUnbalanced)
              .addAllSizeEnteringNodes(toProto(sizeEntering))
              .addAllSizeLeavingNodes(toProto(sizeLeaving));
      if (type != null) {
        builder.setStorageType(StorageTypeUtils.getStorageTypeProto(type));
      }
      return builder.build();
    }

    private static List<NodeTransferInfoProto> toProto(Map<DatanodeID, Long> traffic) {
      return traffic.entrySet().stream()
          .map(entry -> NodeTransferInfoProto.newBuilder()
              .setUuid(entry.getKey().toString())
              .setDataVolume(entry.getValue())
              .build())
          .sorted(Comparator.comparing(NodeTransferInfoProto::getUuid))
          .collect(Collectors.toList());
    }

    private synchronized void snapshot(MetricsRecordBuilder record) {
      Set<DatanodeID> involved = new HashSet<>(sizeEntering.keySet());
      involved.addAll(sizeLeaving.keySet());
      record
          .addGauge(Interns.info("NumContainerMovesScheduledInLatestIteration",
              "Container moves scheduled in the latest iteration"), scheduled)
          .addGauge(Interns.info("NumContainerMovesCompletedInLatestIteration",
              "Container moves completed in the latest iteration"), completed)
          .addGauge(Interns.info("NumContainerMovesFailedInLatestIteration",
              "Container moves that failed in the latest iteration"), failed)
          .addGauge(Interns.info("NumContainerMovesTimeoutInLatestIteration",
              "Container moves that timed out in the latest iteration"), timeout)
          .addGauge(Interns.info("DataSizeScheduledBytesInLatestIteration",
              "Bytes scheduled to move in the latest iteration"), sizeScheduled)
          .addGauge(Interns.info("DataSizeMovedBytesInLatestIteration",
              "Bytes moved in the latest iteration"), dataSizeMoved)
          .addGauge(Interns.info("NumDatanodesInvolvedInLatestIteration",
              "Datanodes that sent or received data in the latest iteration"), (long) involved.size())
          .addGauge(Interns.info("NumDatanodesUnbalanced",
              "Datanodes found unbalanced for this storage type in the latest iteration"), numDatanodesUnbalanced)
          .addGauge(Interns.info("DataSizeUnbalancedBytes",
              "Bytes causing imbalance for this storage type in the latest iteration"), dataSizeUnbalanced)
          .addCounter(Interns.info("NumContainerMovesScheduled",
              "Container moves scheduled across finished iterations"), totalScheduled)
          .addCounter(Interns.info("NumContainerMovesCompleted",
              "Container moves completed across finished iterations"), totalCompleted)
          .addCounter(Interns.info("NumContainerMovesFailed",
              "Container moves that failed across finished iterations"), totalFailed)
          .addCounter(Interns.info("NumContainerMovesTimeout",
              "Container moves that timed out across finished iterations"), totalTimeout)
          .addCounter(Interns.info("DataSizeMovedBytes",
              "Bytes moved across finished iterations"), totalDataSizeMoved);
    }
  }
}
