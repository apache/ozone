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

package org.apache.hadoop.hdds.scm.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartContainerBalancerResponseProto;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerListResult;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;

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
public interface ScmClient extends Closeable {
  /**
   * Creates a Container on SCM and returns the pipeline.
   * @return ContainerInfo
   * @throws IOException
   */
  ContainerWithPipeline createContainer(String owner) throws IOException;

  /**
   * Gets a container by Name -- Throws if the container does not exist.
   * @param containerId - Container ID
   * @return Pipeline
   * @throws IOException
   */
  ContainerInfo getContainer(long containerId) throws IOException;

  /**
   * Gets a container by Name -- Throws if the container does not exist.
   * @param containerId - Container ID
   * @return ContainerWithPipeline
   * @throws IOException
   */
  ContainerWithPipeline getContainerWithPipeline(long containerId)
      throws IOException;

  /**
   * Gets the list of ReplicaInfo known by SCM for a given container.
   * @param containerId - The Container ID
   * @return List of ContainerReplicaInfo for the container or an empty list
   *         if none.
   * @throws IOException
   */
  List<ContainerReplicaInfo> getContainerReplicas(
      long containerId) throws IOException;

  /**
   * Close a container.
   *
   * @param containerId - ID of the container.
   * @throws IOException
   */
  void closeContainer(long containerId) throws IOException;

  /**
   * Deletes an existing container.
   * @param containerId - ID of the container.
   * @param pipeline - Pipeline that represents the container.
   * @param force - true to forcibly delete the container.
   * @throws IOException
   */
  void deleteContainer(long containerId, Pipeline pipeline, boolean force)
      throws IOException;

  /**
   * Deletes an existing container.
   * @param containerId - ID of the container.
   * @param force - true to forcibly delete the container.
   * @throws IOException
   */
  void deleteContainer(long containerId, boolean force) throws IOException;

  /**
   * Lists a range of containers and get their info.
   *
   * @param startContainerID start containerID.
   * @param count count must be {@literal >} 0.
   *
   * @return a list of containers capped by max count allowed
   * in "ozone.scm.container.list.max.count" and total number of containers.
   * @throws IOException
   */
  ContainerListResult listContainer(long startContainerID,
      int count) throws IOException;

  /**
   * Lists a range of containers and get their info.
   *
   * @param startContainerID start containerID.
   * @param count count must be {@literal >} 0.
   * @param state Container of this state will be returned.
   * @param replicationConfig container replication Config.
   * @return a list of containers capped by max count allowed
   * in "ozone.scm.container.list.max.count" and total number of containers.
   * @throws IOException
   */
  ContainerListResult listContainer(long startContainerID, int count,
      HddsProtos.LifeCycleState state,
      HddsProtos.ReplicationType replicationType,
      ReplicationConfig replicationConfig)
      throws IOException;

  /**
   * Read meta data from an existing container.
   * @param containerID - ID of the container.
   * @param pipeline - Pipeline where the container is located.
   * @return ContainerInfo
   * @throws IOException
   */
  ContainerDataProto readContainer(long containerID, Pipeline pipeline)
      throws IOException;

  /**
   * Read meta data from an existing container.
   * @param containerID - ID of the container.
   * @return ContainerInfo
   * @throws IOException
   */
  ContainerDataProto readContainer(long containerID)
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
  @Deprecated
  ContainerWithPipeline createContainer(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor replicationFactor,
      String owner) throws IOException;

  ContainerWithPipeline createContainer(ReplicationConfig replicationConfig, String owner) throws IOException;

  /**
   * Gets the list of underReplicated and unClosed containers on a decommissioning node.
   *
   * @param dn - Datanode detail
   * @return Lists of underReplicated and Unclosed containers
   */
  Map<String, List<ContainerID>> getContainersOnDecomNode(DatanodeDetails dn) throws IOException;

  /**
   * Returns a set of Nodes that meet a query criteria. Passing null for opState
   * or nodeState acts like a wild card, returning all nodes in that state.
   * @param opState - Operational State of the node, eg IN_SERVICE,
   *                DECOMMISSIONED, etc
   * @param nodeState - Health of the nodeCriteria that we want the node to
   *                  have, eg HEALTHY, STALE etc
   * @param queryScope - Query scope - Cluster or pool.
   * @param poolName - if it is pool, a pool name is required.
   * @return A set of nodes that meet the requested criteria.
   * @throws IOException
   */
  List<HddsProtos.Node> queryNode(HddsProtos.NodeOperationalState opState,
      HddsProtos.NodeState nodeState, HddsProtos.QueryScope queryScope,
      String poolName) throws IOException;

  /**
   * Returns a node with the given UUID.
   * @param uuid - datanode uuid string
   * @return A nodes that matches the requested UUID.
   * @throws IOException
   */
  HddsProtos.Node queryNode(UUID uuid) throws IOException;

  /**
   * Allows a list of hosts to be decommissioned. The hosts are identified
   * by their hostname and optionally port in the format foo.com:port.
   * @param hosts A list of hostnames, optionally with port
   * @param force true to forcefully decommission Datanodes
   * @throws IOException
   * @return A list of DatanodeAdminError for any hosts which failed to
   *         decommission
   */
  List<DatanodeAdminError> decommissionNodes(List<String> hosts, boolean force)
      throws IOException;

  /**
   * Allows a list of hosts in maintenance or decommission states to be placed
   * back in service. The hosts are identified by their hostname and optionally
   * port in the format foo.com:port.
   * @param hosts A list of hostnames, optionally with port
   * @return A list of DatanodeAdminError for any hosts which failed to
   *         recommission
   * @throws IOException
   */
  List<DatanodeAdminError> recommissionNodes(List<String> hosts)
      throws IOException;

  /**
   * Place the list of datanodes into maintenance mode. If a non-zero endDtm
   * is passed, the hosts will automatically exit maintenance mode after the
   * given time has passed. Passing an end time of zero means the hosts will
   * remain in maintenance indefinitely.
   * The hosts are identified by their hostname and optionally port in the
   * format foo.com:port.
   * @param hosts A list of hostnames, optionally with port
   * @param endHours The number of hours from now which maintenance will end or
   *                 zero if maintenance must be manually ended.
   * @return A list of DatanodeAdminError for any hosts which failed to
   *         end maintenance.
   * @throws IOException
   */
  List<DatanodeAdminError> startMaintenanceNodes(List<String> hosts,
      int endHours, boolean force) throws IOException;

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

  /**
   * Returns the list of active Pipelines.
   *
   * @return list of Pipeline
   * @throws IOException in case of any exception
   */
  List<Pipeline> listPipelines() throws IOException;

  /**
   * Returns a pipeline with ID, if present.
   * @return pipeline
   * @throws IOException in case of exception
   */
  Pipeline getPipeline(HddsProtos.PipelineID pipelineID) throws IOException;

  /**
   * Activates the pipeline given a pipeline ID.
   *
   * @param pipelineID PipelineID to activate.
   * @throws IOException In case of exception while activating the pipeline
   */
  void activatePipeline(HddsProtos.PipelineID pipelineID) throws IOException;

  /**
   * Deactivates the pipeline given a pipeline ID.
   *
   * @param pipelineID PipelineID to deactivate.
   * @throws IOException In case of exception while deactivating the pipeline
   */
  void deactivatePipeline(HddsProtos.PipelineID pipelineID) throws IOException;

  /**
   * Closes the pipeline given a pipeline ID.
   *
   * @param pipelineID PipelineID to close.
   * @throws IOException In case of exception while closing the pipeline
   */
  void closePipeline(HddsProtos.PipelineID pipelineID) throws IOException;

  /**
   * Check if SCM is in safe mode.
   *
   * @return Returns true if SCM is in safe mode else returns false.
   * @throws IOException
   */
  boolean inSafeMode() throws IOException;

  /**
   * Get the safe mode status of all rules.
   *
   * @return map of rule statuses.
   * @throws IOException
   */
  Map<String, Pair<Boolean, String>> getSafeModeRuleStatuses()
      throws IOException;

  /**
   * Force SCM out of safe mode.
   *
   * @return returns true if operation is successful.
   * @throws IOException
   */
  boolean forceExitSafeMode() throws IOException;

  /**
   * Start ReplicationManager.
   */
  void startReplicationManager() throws IOException;

  /**
   * Stop ReplicationManager.
   */
  void stopReplicationManager() throws IOException;

  /**
   * Returns ReplicationManager status.
   *
   * @return True if ReplicationManager is running, false otherwise.
   */
  boolean getReplicationManagerStatus() throws IOException;

  /**
   * Returns the latest container summary report generated by Replication
   * Manager.
   * @return The latest ReplicationManagerReport.
   * @throws IOException
   */
  ReplicationManagerReport getReplicationManagerReport() throws IOException;

  /**
   * Start ContainerBalancer.
   */
  @SuppressWarnings("checkstyle:parameternumber")
  StartContainerBalancerResponseProto startContainerBalancer(
      Optional<Double> threshold,
      Optional<Integer> iterations,
      Optional<Integer> maxDatanodesPercentageToInvolvePerIteration,
      Optional<Long> maxSizeToMovePerIterationInGB,
      Optional<Long> maxSizeEnteringTargetInGB,
      Optional<Long> maxSizeLeavingSourceInGB,
      Optional<Integer> balancingInterval,
      Optional<Integer> moveTimeout,
      Optional<Integer> moveReplicationTimeout,
      Optional<Boolean> networkTopologyEnable,
      Optional<String> includeNodes,
      Optional<String> excludeNodes) throws IOException;

  /**
   * Stop ContainerBalancer.
   */
  void stopContainerBalancer() throws IOException;

  /**
   * Returns ContainerBalancer status.
   *
   * @return True if ContainerBalancer is running, false otherwise.
   */
  boolean getContainerBalancerStatus() throws IOException;

  ContainerBalancerStatusInfoResponseProto getContainerBalancerStatusInfo() throws IOException;

  /**
   * returns the list of SCM peer roles. Currently only include peer address.
   */
  List<String> getScmRoles() throws IOException;

  /**
   * Force generates new secret keys (rotate).
   *
   * @param force boolean flag that forcefully rotates the key on demand
   * @throws IOException
   */
  boolean rotateSecretKeys(boolean force) throws IOException;

  /**
   * Transfer the raft leadership.
   *
   * @param newLeaderId  the newLeaderId of the target expected leader
   * @throws IOException
   */
  void transferLeadership(String newLeaderId) throws IOException;

  /**
   * Get deleted block summary.
   * @throws IOException
   */
  DeletedBlocksTransactionSummary getDeletedBlockSummary() throws IOException;

  /**
   * Get usage information of datanode by address or uuid.
   *
   * @param address datanode address String
   * @param uuid datanode uuid String
   * @return List of DatanodeUsageInfoProto. Each element contains info such as
   * capacity, SCMused, and remaining space.
   * @throws IOException
   */
  List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(String address,
                                                               String uuid)
      throws IOException;

  /**
   * Get usage information of most or least used datanodes.
   *
   * @param mostUsed true if most used, false if least used
   * @param count Integer number of nodes to get info for
   * @return List of DatanodeUsageInfoProto. Each element contains info such as
   * capacity, SCMUsed, and remaining space.
   * @throws IOException
   */
  List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(
      boolean mostUsed, int count) throws IOException;

  StatusAndMessages finalizeScmUpgrade(String upgradeClientID)
      throws IOException;

  StatusAndMessages queryUpgradeFinalizationProgress(
      String upgradeClientID, boolean force, boolean readonly)
      throws IOException;

  DecommissionScmResponseProto decommissionScm(
      String scmId) throws IOException;

  String getMetrics(String query) throws IOException;

  /**
   * Trigger a reconcile command to datanodes for a container ID.
   *
   * @param containerID The ID of the container to reconcile.
   * @throws IOException On error
   */
  void reconcileContainer(long containerID) throws IOException;

  /**
   * Set or unset the ACK_MISSING state for a container.
   *
   * @param containerId The ID of the container.
   * @param acknowledge true to set ACK_MISSING, false to unset to MISSING.
   * @throws IOException
   */
  void setAckMissingContainer(long containerId, boolean acknowledge) throws IOException;
}
