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

package org.apache.hadoop.hdds.scm.protocol;

import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartContainerBalancerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.Type;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerListResult;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.Token;

/**
 * ContainerLocationProtocol is used by an HDFS node to find the set of nodes
 * that currently host a container.
 */
@KerberosInfo(serverPrincipal = ScmConfig.ConfigStrings
      .HDDS_SCM_KERBEROS_PRINCIPAL_KEY)
public interface StorageContainerLocationProtocol extends Closeable {

  // Accessed and checked via reflection in Hadoop RPC - changing it is incompatible
  @SuppressWarnings({"checkstyle:ConstantName", "unused"})
  /**
   * Version 1: Initial version.
   */
  long versionID = 1L;

  /**
   * Admin command should take effect on all SCM instance.
   */
  Set<Type> ADMIN_COMMAND_TYPE = Collections.unmodifiableSet(EnumSet.of(
      Type.StartReplicationManager,
      Type.StopReplicationManager,
      Type.ForceExitSafeMode));

  /**
   * Asks SCM where a container should be allocated. SCM responds with the
   * set of datanodes that should be used creating this container.
   *
   */
  ContainerWithPipeline allocateContainer(
      HddsProtos.ReplicationType replicationType,
      HddsProtos.ReplicationFactor factor, String owner)
      throws IOException;

  ContainerWithPipeline allocateContainer(ReplicationConfig replicationConfig, String owner) throws IOException;

  /**
   * Ask SCM the location of the container. SCM responds with a group of
   * nodes where this container and its replicas are located.
   *
   * @param containerID - ID of the container.
   * @return ContainerInfo - the container info such as where the pipeline
   *                         is located.
   * @throws IOException
   */
  ContainerInfo getContainer(long containerID) throws IOException;

  /**
   * Ask SCM the location of the container. SCM responds with a group of
   * nodes where this container and its replicas are located.
   *
   * @param containerID - ID of the container.
   * @return ContainerWithPipeline - the container info with the pipeline.
   * @throws IOException
   */
  ContainerWithPipeline getContainerWithPipeline(long containerID)
      throws IOException;

  /**
   * Gets the list of ReplicaInfo known by SCM for a given container.
   * @param containerId ID of the container
   * @return List of ReplicaInfo for the container or an empty list if none.
   * @throws IOException
   */
  List<HddsProtos.SCMContainerReplicaProto> getContainerReplicas(
      long containerId, int clientVersion) throws IOException;

  /**
   * Ask SCM the location of a batch of containers. SCM responds with a group of
   * nodes where these containers and their replicas are located.
   *
   * @param containerIDs - IDs of a batch of containers.
   * @return List of ContainerWithPipeline
   * - the container info with the pipeline.
   * @throws IOException
   */
  List<ContainerWithPipeline> getContainerWithPipelineBatch(
      Iterable<? extends Long> containerIDs) throws IOException;

  /**
   * Ask SCM which containers of the given list exist.
   *
   * @param containerIDs - IDs of a batch of containers.
   * @return List of ContainerWithPipeline that exist in SCM
   * - the container info with the pipeline.
   */
  List<ContainerWithPipeline> getExistContainerWithPipelinesInBatch(
      List<Long> containerIDs);

  /**
   * Ask SCM a list of containers with a range of container names
   * and the limit of count.
   * Search container names between start name(exclusive), and
   * use prefix name to filter the result. the max size of the
   * searching range cannot exceed the value of count.
   *
   * @param startContainerID start container ID.
   * @param count count, if count {@literal <} 0, the max size is unlimited.(
   *              Usually the count will be replace with a very big
   *              value instead of being unlimited in case the db is very big)
   *
   * @return a list of containers capped by max count allowed
   * in "ozone.scm.container.list.max.count" and total number of containers.
   * @throws IOException
   */
  ContainerListResult listContainer(long startContainerID,
      int count) throws IOException;

  /**
   * Ask SCM a list of containers with a range of container names
   * and the limit of count.
   * Search container names between start name(exclusive), and
   * use prefix name to filter the result. the max size of the
   * searching range cannot exceed the value of count.
   *
   * @param startContainerID start container ID.
   * @param count count, if count {@literal <} 0, the max size is unlimited.(
   *              Usually the count will be replace with a very big
   *              value instead of being unlimited in case the db is very big)
   * @param state Container with this state will be returned.
   *
   * @return a list of containers capped by max count allowed
   * in "ozone.scm.container.list.max.count" and total number of containers.
   * @throws IOException
   */
  ContainerListResult listContainer(long startContainerID,
      int count, HddsProtos.LifeCycleState state) throws IOException;

  /**
   * Ask SCM a list of containers with a range of container names
   * and the limit of count.
   * Search container names between start name(exclusive), and
   * use prefix name to filter the result. the max size of the
   * searching range cannot exceed the value of count.
   *
   * @param startContainerID start container ID.
   * @param count count, if count {@literal <} 0, the max size is unlimited.(
   *              Usually the count will be replace with a very big
   *              value instead of being unlimited in case the db is very big)
   * @param state Container with this state will be returned.
   * @param factor Container factor
   * @return a list of containers capped by max count allowed
   * in "ozone.scm.container.list.max.count" and total number of containers.
   * @throws IOException
   */
  ContainerListResult listContainer(long startContainerID,
      int count, HddsProtos.LifeCycleState state,
      HddsProtos.ReplicationFactor factor) throws IOException;

  /**
   * Ask SCM for a list of containers with a range of container ID, state
   * and replication config, and the limit of count.
   * The containers are returned from startID (exclusive), and
   * filtered by state and replication config. The returned list is limited to
   * count entries.
   *
   * @param startContainerID start container ID.
   * @param count count, if count {@literal <} 0, the max size is unlimited.(
   *              Usually the count will be replace with a very big
   *              value instead of being unlimited in case the db is very big)
   * @param state Container with this state will be returned.
   * @param replicationConfig Replication config for the containers
   * @return a list of containers capped by max count allowed
   * in "ozone.scm.container.list.max.count" and total number of containers.
   * @throws IOException
   */
  ContainerListResult listContainer(long startContainerID,
      int count, HddsProtos.LifeCycleState state,
      HddsProtos.ReplicationType replicationType,
      ReplicationConfig replicationConfig) throws IOException;

  /**
   * Deletes a container in SCM.
   *
   * @param containerID
   * @throws IOException
   *   if failed to delete the container mapping from db store
   *   or container doesn't exist.
   */
  void deleteContainer(long containerID) throws IOException;

  /**
   * Gets the list of underReplicated and unClosed containers on a decommissioning node.
   *
   * @param dn - Datanode detail
   * @return Lists of underReplicated and unClosed containers
   */
  Map<String, List<ContainerID>> getContainersOnDecomNode(DatanodeDetails dn) throws IOException;

  /**
   *  Queries a list of Node Statuses. Passing a null for either opState or
   *  state acts like a wildcard returning all nodes in that state.
   * @param opState The node operational state
   * @param state The node health
   * @param clientVersion Client's version number
   * @return List of Datanodes.
   * @see org.apache.hadoop.ozone.ClientVersion
   */
  List<HddsProtos.Node> queryNode(HddsProtos.NodeOperationalState opState,
      HddsProtos.NodeState state, HddsProtos.QueryScope queryScope,
      String poolName, int clientVersion) throws IOException;

  HddsProtos.Node queryNode(UUID uuid) throws IOException;

  List<DatanodeAdminError> decommissionNodes(List<String> nodes, boolean force)
      throws IOException;

  List<DatanodeAdminError> recommissionNodes(List<String> nodes)
      throws IOException;

  List<DatanodeAdminError> startMaintenanceNodes(List<String> nodes,
      int endInHours, boolean force) throws IOException;

  /**
   * Close a container.
   *
   * @param containerID ID of the container to close
   * @throws IOException in case of any Exception
   */
  void closeContainer(long containerID) throws IOException;

  /**
   * Creates a replication pipeline of a specified type.
   * @param type - replication type
   * @param factor - factor 1 or 3
   * @param nodePool - optional machine list to build a pipeline.
   * @throws IOException
   */
  Pipeline createReplicationPipeline(HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor, HddsProtos.NodePool nodePool)
      throws IOException;

  /**
   * Returns the list of active Pipelines.
   *
   * @return list of Pipeline
   *
   * @throws IOException in case of any exception
   */
  List<Pipeline> listPipelines() throws IOException;

  /**
   * Returns Pipeline with given ID if present.
   *
   * @return Pipeline
   *
   * @throws IOException in case of any exception
   */
  Pipeline getPipeline(HddsProtos.PipelineID pipelineID) throws IOException;

  /**
   * Activates a dormant pipeline.
   *
   * @param pipelineID ID of the pipeline to activate.
   * @throws IOException in case of any Exception
   */
  void activatePipeline(HddsProtos.PipelineID pipelineID) throws IOException;

  /**
   * Deactivates an active pipeline.
   *
   * @param pipelineID ID of the pipeline to deactivate.
   * @throws IOException in case of any Exception
   */
  void deactivatePipeline(HddsProtos.PipelineID pipelineID) throws IOException;

  /**
   * Closes a pipeline given the pipelineID.
   *
   * @param pipelineID ID of the pipeline to demolish
   * @throws IOException
   */
  void closePipeline(HddsProtos.PipelineID pipelineID) throws IOException;

  /**
   * Returns information about SCM.
   *
   * @return {@link ScmInfo}
   * @throws IOException
   */
  ScmInfo getScmInfo() throws IOException;

  /**
   * Transfer the raft leadership.
   *
   * @param newLeaderId  the newLeaderId of the target expected leader
   * @throws IOException
   */
  void transferLeadership(String newLeaderId) throws IOException;

  /**
   * Return the failed transactions of the Deleted blocks. A transaction is
   * considered to be failed if it has been sent more than MAX_RETRY limit
   * and its count is reset to -1.
   *
   * @param count Maximum num of returned transactions, if {@literal < 0}. return all.
   * @param startTxId The least transaction id to start with.
   * @return a list of failed deleted block transactions.
   * @throws IOException
   */
  @Deprecated
  List<DeletedBlocksTransactionInfo> getFailedDeletedBlockTxn(int count,
      long startTxId) throws IOException;

  /**
   * Reset the failed deleted block retry count.
   *
   * @param txIDs transactionId list to be reset
   * @return num of successful reset
   * @throws IOException
   */
  @Deprecated
  int resetDeletedBlockRetryCount(List<Long> txIDs) throws IOException;


  /**
   * Get deleted block summary.
   * @throws IOException
   */
  @Nullable
  DeletedBlocksTransactionSummary getDeletedBlockSummary() throws IOException;

  /**
   * Check if SCM is in safe mode.
   *
   * @return Returns true if SCM is in safe mode else returns false.
   * @throws IOException
   */
  boolean inSafeMode() throws IOException;

  Map<String, Pair<Boolean, String>> getSafeModeRuleStatuses()
      throws IOException;

  /**
   * Force SCM out of Safe mode.
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
   * @return {@link StartContainerBalancerResponseProto} that contains the
   * start status and an optional message.
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
   * Get Datanode usage information by ip or hostname or uuid.
   *
   * @param address datanode IP address or Hostname String
   * @param uuid datanode UUID String
   * @param clientVersion Client's version number
   * @return List of DatanodeUsageInfoProto. Each element contains info such as
   * capacity, SCMused, and remaining space.
   * @throws IOException
   * @see org.apache.hadoop.ozone.ClientVersion
   */
  List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(
      String address, String uuid, int clientVersion) throws IOException;

  /**
   * Get usage information of most or least used datanodes.
   *
   * @param mostUsed true if most used, false if least used
   * @param count Integer number of nodes to get info for
   * @param clientVersion Client's version number
   * @return List of DatanodeUsageInfoProto. Each element contains info such as
   * capacity, SCMUsed, and remaining space.
   * @throws IOException
   * @see org.apache.hadoop.ozone.ClientVersion
   */
  List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(
      boolean mostUsed, int count, int clientVersion) throws IOException;

  StatusAndMessages finalizeScmUpgrade(String upgradeClientID)
      throws IOException;

  StatusAndMessages queryUpgradeFinalizationProgress(
      String upgradeClientID, boolean force, boolean readonly)
      throws IOException;

  /**
   * Obtain a token which can be used to let datanodes verify authentication of
   * commands operating on {@code containerID}.
   */
  Token<?> getContainerToken(ContainerID containerID) throws IOException;

  long getContainerCount() throws IOException;

  long getContainerCount(HddsProtos.LifeCycleState state)
      throws IOException;

  List<ContainerInfo> getListOfContainers(
      long startContainerID, int count, HddsProtos.LifeCycleState state)
      throws IOException;

  DecommissionScmResponseProto decommissionScm(
      String scmId) throws IOException;

  String getMetrics(String query) throws IOException;

  /**
   * Trigger a reconcile command to datanodes for the current container ID.
   *
   * @param containerID The ID of the container to reconcile.
   * @throws IOException On error
   */
  void reconcileContainer(long containerID) throws IOException;
}
