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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;

/**
 * Information about balancer task iteration.
 */
public class ContainerBalancerTaskIterationStatusInfo {

  private final IterationInfo iterationInfo;
  private final ContainerMoveInfo containerMoveInfo;
  private final DataMoveInfo dataMoveInfo;

  public ContainerBalancerTaskIterationStatusInfo(
      IterationInfo iterationInfo,
      ContainerMoveInfo containerMoveInfo,
      DataMoveInfo dataMoveInfo) {
    this.iterationInfo = iterationInfo;
    this.containerMoveInfo = containerMoveInfo;
    this.dataMoveInfo = dataMoveInfo;
  }

  /**
   * Get the number of iterations.
   * @return iteration number
   */
  public Integer getIterationNumber() {
    return iterationInfo.getIterationNumber();
  }

  /**
   * Get the iteration result.
   * @return iteration result
   */
  public String getIterationResult() {
    return iterationInfo.getIterationResult();
  }

  /**
   * Get the size of the bytes that are scheduled to move in the iteration.
   * @return size in bytes
   */
  public long getSizeScheduledForMove() {
    return dataMoveInfo.getSizeScheduledForMove();
  }

  /**
   * Get the size of the bytes that were moved in the iteration.
   * @return size in bytes
   */
  public long getDataSizeMoved() {
    return dataMoveInfo.getDataSizeMoved();
  }

  /**
   * Get the number of containers scheduled to move.
   * @return number of containers scheduled to move
   */
  public long getContainerMovesScheduled() {
    return containerMoveInfo.getContainerMovesScheduled();
  }

  /**
   * Get the number of successfully moved containers.
   * @return number of successfully moved containers
   */
  public long getContainerMovesCompleted() {
    return containerMoveInfo.getContainerMovesCompleted();
  }

  /**
   * Get the number of containers that were not moved successfully.
   * @return number of unsuccessfully moved containers
   */
  public long getContainerMovesFailed() {
    return containerMoveInfo.getContainerMovesFailed();
  }

  /**
   * Get the number of containers moved with a timeout.
   * @return number of moved with timeout containers
   */
  public long getContainerMovesTimeout() {
    return containerMoveInfo.getContainerMovesTimeout();
  }

  /**
   * Get a map of the node IDs and the corresponding data sizes moved to each node.
   * @return nodeId to size entering from node map
   */
  public Map<DatanodeID, Long> getSizeEnteringNodes() {
    return dataMoveInfo.getSizeEnteringNodes();
  }

  /**
   * Get a map of the node IDs and the corresponding data sizes moved from each node.
   * @return nodeId to size leaving from node map
   */
  public Map<DatanodeID, Long> getSizeLeavingNodes() {
    return dataMoveInfo.getSizeLeavingNodes();
  }

  /**
   * Get the iteration duration.
   * @return iteration duration
   */
  public Long getIterationDuration() {
    return iterationInfo.getIterationDuration();
  }

  /**
   * Converts an instance into the protobuf compatible object.
   * @return proto representation
   */
  public StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfoProto toProto() {
    return StorageContainerLocationProtocolProtos.ContainerBalancerTaskIterationStatusInfoProto.newBuilder()
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
   * Converts an instance into the protobuf compatible object.
   * @param nodes node id to node traffic size
   * @return node transfer info proto representation
   */
  private List<StorageContainerLocationProtocolProtos.NodeTransferInfoProto> mapToProtoNodeTransferInfo(
      Map<DatanodeID, Long> nodes
  ) {
    return nodes.entrySet()
        .stream()
        .map(entry -> StorageContainerLocationProtocolProtos.NodeTransferInfoProto.newBuilder()
            .setUuid(entry.getKey().toString())
            .setDataVolume(entry.getValue())
            .build()
        )
        .collect(Collectors.toList());
  }
}


