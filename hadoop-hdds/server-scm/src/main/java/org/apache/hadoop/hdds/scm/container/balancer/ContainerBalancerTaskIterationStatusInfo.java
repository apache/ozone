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

package org.apache.hadoop.hdds.scm.container.balancer;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Information about balancer task iteration.
 */
public class ContainerBalancerTaskIterationStatusInfo {
  private final Integer iterationNumber;
  private final String iterationResult;
  private final long iterationDuration;
  private final long sizeScheduledForMove;
  private final long dataSizeMoved;
  private final long containerMovesScheduled;
  private final long containerMovesCompleted;
  private final long containerMovesFailed;
  private final long containerMovesTimeout;
  private final Map<UUID, Long> sizeEnteringNodes;
  private final Map<UUID, Long> sizeLeavingNodes;

  @SuppressWarnings("checkstyle:ParameterNumber")
  public ContainerBalancerTaskIterationStatusInfo(
      Integer iterationNumber,
      String iterationResult,
      long iterationDuration,
      long sizeScheduledForMove,
      long dataSizeMoved,
      long containerMovesScheduled,
      long containerMovesCompleted,
      long containerMovesFailed,
      long containerMovesTimeout,
      Map<UUID, Long> sizeEnteringNodes,
      Map<UUID, Long> sizeLeavingNodes) {
    this.iterationNumber = iterationNumber;
    this.iterationResult = iterationResult;
    this.iterationDuration = iterationDuration;
    this.sizeScheduledForMove = sizeScheduledForMove;
    this.dataSizeMoved = dataSizeMoved;
    this.containerMovesScheduled = containerMovesScheduled;
    this.containerMovesCompleted = containerMovesCompleted;
    this.containerMovesFailed = containerMovesFailed;
    this.containerMovesTimeout = containerMovesTimeout;
    this.sizeEnteringNodes = sizeEnteringNodes;
    this.sizeLeavingNodes = sizeLeavingNodes;
  }

  /**
   * Get the number of iterations.
   * @return iteration number
   */
  public Integer getIterationNumber() {
    return iterationNumber;
  }

  /**
   * Get the iteration result.
   * @return iteration result
   */
  public String getIterationResult() {
    return iterationResult;
  }

  /**
   * Get size in bytes scheduled to move in the iteration.
   * @return size in bytes
   */
  public long getSizeScheduledForMove() {
    return sizeScheduledForMove;
  }

  /**
   * Get size in bytes moved in the iteration.
   * @return size in bytes
   */
  public long getDataSizeMoved() {
    return dataSizeMoved;
  }

  /**
   * Get the number of containers scheduled to move.
   * @return number of containers scheduled to move
   */
  public long getContainerMovesScheduled() {
    return containerMovesScheduled;
  }

  /**
   * Get the number of successfully moved containers.
   * @return number of successfully moved containers
   */
  public long getContainerMovesCompleted() {
    return containerMovesCompleted;
  }

  /**
   * Get the number of unsuccessfully moved containers.
   * @return number of unsuccessfully moved containers
   */
  public long getContainerMovesFailed() {
    return containerMovesFailed;
  }

  /**
   * Get the number of moved with timeout containers.
   * @return number of moved with timeout containers
   */
  public long getContainerMovesTimeout() {
    return containerMovesTimeout;
  }

  /**
   * Get a map of the id node and the data size moved to the node.
   * @return nodeId to size entering from node map
   */
  public Map<UUID, Long> getSizeEnteringNodes() {
    return sizeEnteringNodes;
  }

  /**
   * Get a map of the id node and the data size moved from the node.
   * @return nodeId to size leaving from node map
   */
  public Map<UUID, Long> getSizeLeavingNodes() {
    return sizeLeavingNodes;
  }

  /**
   * Get iteration duration.
   * @return iteration duration
   */
  public long getIterationDuration() {
    return iterationDuration;
  }

  /**
   * Map to proto.
   * @return proto representation
   */
  public StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfo toProto() {
    return StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfo.newBuilder()
        .setIterationNumber(getIterationNumber())
        .setIterationResult(Optional.ofNullable(getIterationResult()).orElse(""))
        .setIterationDuration(getIterationDuration())
        .setSizeScheduledForMove(getSizeScheduledForMove())
        .setDataSizeMoved(getDataSizeMoved())
        .setContainerMovesScheduled(getContainerMovesScheduled())
        .setContainerMovesCompleted(getContainerMovesCompleted())
        .setContainerMovesFailed(getContainerMovesFailed())
        .setContainerMovesTimeout(getContainerMovesTimeout())
        .addAllSizeEnteringNodes(
            mapToProtoNodeTransferInfo(getSizeEnteringNodes())
        )
        .addAllSizeLeavingNodes(
            mapToProtoNodeTransferInfo(getSizeLeavingNodes())
        )
        .build();
  }

  /**
   * Map to proto node transfer info.
   * @param nodes node id to node traffic size
   * @return node transfer info proto representation
   */
  private List<StorageContainerLocationProtocolProtos.NodeTransferInfo> mapToProtoNodeTransferInfo(
      Map<UUID, Long> nodes
  ) {
    return nodes.entrySet()
        .stream()
        .map(entry -> StorageContainerLocationProtocolProtos.NodeTransferInfo.newBuilder()
            .setUuid(entry.getKey().toString())
            .setDataVolume(entry.getValue())
            .build()
        )
        .collect(Collectors.toList());
  }
}


