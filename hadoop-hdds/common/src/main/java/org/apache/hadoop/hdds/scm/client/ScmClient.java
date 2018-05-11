/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.client;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos
    .ContainerData;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

/**
 * The interface to call into underlying container layer.
 *
 * Written as interface to allow easy testing: implement a mock container layer
 * for standalone testing of CBlock API without actually calling into remote
 * containers. Actual container layer can simply re-implement this.
 *
 * NOTE this is temporarily needed class. When SCM containers are full-fledged,
 * this interface will likely be removed.
 */
@InterfaceStability.Unstable
public interface ScmClient {
  /**
   * Creates a Container on SCM and returns the pipeline.
   * @return ContainerInfo
   * @throws IOException
   */
  ContainerInfo createContainer(String owner) throws IOException;

  /**
   * Gets a container by Name -- Throws if the container does not exist.
   * @param containerId - Container ID
   * @return Pipeline
   * @throws IOException
   */
  ContainerInfo getContainer(long containerId) throws IOException;

  /**
   * Close a container.
   *
   * @param containerId - ID of the container.
   * @param pipeline - Pipeline where the container is located.
   * @throws IOException
   */
  void closeContainer(long containerId, Pipeline pipeline) throws IOException;

  /**
   * Deletes an existing container.
   * @param containerId - ID of the container.
   * @param pipeline - Pipeline that represents the container.
   * @param force - true to forcibly delete the container.
   * @throws IOException
   */
  void deleteContainer(long containerId, Pipeline pipeline, boolean force) throws IOException;

  /**
   * Lists a range of containers and get their info.
   *
   * @param startContainerID start containerID.
   * @param count count must be > 0.
   *
   * @return a list of pipeline.
   * @throws IOException
   */
  List<ContainerInfo> listContainer(long startContainerID,
      int count) throws IOException;

  /**
   * Read meta data from an existing container.
   * @param containerID - ID of the container.
   * @param pipeline - Pipeline where the container is located.
   * @return ContainerInfo
   * @throws IOException
   */
  ContainerData readContainer(long containerID, Pipeline pipeline)
      throws IOException;

  /**
   * Gets the container size -- Computed by SCM from Container Reports.
   * @param containerID - ID of the container.
   * @return number of bytes used by this container.
   * @throws IOException
   */
  long getContainerSize(long containerID) throws IOException;

  /**
   * Creates a Container on SCM and returns the pipeline.
   * @param type - Replication Type.
   * @param replicationFactor - Replication Factor
   * @return ContainerInfo
   * @throws IOException - in case of error.
   */
  ContainerInfo createContainer(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor replicationFactor,
      String owner) throws IOException;

  /**
   * Returns a set of Nodes that meet a query criteria.
   * @param nodeStatuses - A set of criteria that we want the node to have.
   * @param queryScope - Query scope - Cluster or pool.
   * @param poolName - if it is pool, a pool name is required.
   * @return A set of nodes that meet the requested criteria.
   * @throws IOException
   */
  HddsProtos.NodePool queryNode(EnumSet<HddsProtos.NodeState> nodeStatuses,
      HddsProtos.QueryScope queryScope, String poolName) throws IOException;

  /**
   * Creates a specified replication pipeline.
   * @param type - Type
   * @param factor - Replication factor
   * @param nodePool - Set of machines.
   * @throws IOException
   */
  Pipeline createReplicationPipeline(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, HddsProtos.NodePool nodePool)
      throws IOException;
}
