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

package org.apache.hadoop.hdds.scm.protocolPB;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMCloseContainerResponseProto.Status.CONTAINER_ALREADY_CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMCloseContainerResponseProto.Status.CONTAINER_ALREADY_CLOSING;

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicatedReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionInfo;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.GetScmInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.TransferLeadershipRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.UpgradeFinalizationStatus;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ActivatePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ClosePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainersOnDecomNodeProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DatanodeAdminErrorResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DatanodeUsageInfoRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DatanodeUsageInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DeactivatePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionNodesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionNodesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.FinalizeScmUpgradeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.FinalizeScmUpgradeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ForceExitSafeModeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ForceExitSafeModeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerCountRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerCountResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerReplicasRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerTokenRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerTokenResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineBatchRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainersOnDecomNodeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainersOnDecomNodeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetDeletedBlocksTxnSummaryRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetDeletedBlocksTxnSummaryResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetExistContainerWithPipelinesInBatchRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetMetricsRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetMetricsResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetPipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetSafeModeRuleStatusesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetSafeModeRuleStatusesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.InSafeModeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ListPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ListPipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.NodeQueryRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.NodeQueryResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.PipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.PipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.QueryUpgradeFinalizationProgressRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.QueryUpgradeFinalizationProgressResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.RecommissionNodesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.RecommissionNodesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReconcileContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReplicationManagerReportRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReplicationManagerReportResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReplicationManagerStatusRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReplicationManagerStatusResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMCloseContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMCloseContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMDeleteContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMListContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMListContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SafeModeRuleStatusProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationRequest;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationRequest.Builder;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationResponse;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SingleNodeQueryRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SingleNodeQueryResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartContainerBalancerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartContainerBalancerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartMaintenanceNodesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartMaintenanceNodesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartReplicationManagerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StopContainerBalancerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StopReplicationManagerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.Type;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerListResult;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.proxy.SCMContainerLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc_.ProtobufHelper;
import org.apache.hadoop.ipc_.ProtocolTranslator;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.apache.hadoop.security.token.Token;

/**
 * This class is the client-side translator to translate the requests made on
 * the {@link StorageContainerLocationProtocol} interface to the RPC server
 * implementing {@link StorageContainerLocationProtocolPB}.
 */
@InterfaceAudience.Private
public final class StorageContainerLocationProtocolClientSideTranslatorPB
    implements StorageContainerLocationProtocol, ProtocolTranslator, Closeable {

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final StorageContainerLocationProtocolPB rpcProxy;
  private final SCMContainerLocationFailoverProxyProvider fpp;

  /**
   * Creates a new StorageContainerLocationProtocolClientSideTranslatorPB.
   *
   * @param proxyProvider {@link SCMContainerLocationFailoverProxyProvider}
   */
  public StorageContainerLocationProtocolClientSideTranslatorPB(
      SCMContainerLocationFailoverProxyProvider proxyProvider) {
    Objects.requireNonNull(proxyProvider, "proxyProvider == null");
    this.fpp = proxyProvider;
    this.rpcProxy = (StorageContainerLocationProtocolPB) RetryProxy.create(
        StorageContainerLocationProtocolPB.class,
        fpp,
        fpp.getRetryPolicy());
  }

  /**
   * Helper method to wrap the request and send the message.
   */
  private ScmContainerLocationResponse submitRequest(
      StorageContainerLocationProtocolProtos.Type type,
      Consumer<Builder> builderConsumer) throws IOException {
    final ScmContainerLocationResponse response;
    try {
      Builder builder = ScmContainerLocationRequest.newBuilder()
          .setCmdType(type)
          .setVersion(ClientVersion.CURRENT_VERSION)
          .setTraceID(TracingUtil.exportCurrentSpan());
      builderConsumer.accept(builder);
      ScmContainerLocationRequest wrapper = builder.build();

      response = submitRpcRequest(wrapper);
    } catch (ServiceException ex) {
      throw ProtobufHelper.getRemoteException(ex);
    }
    return response;
  }

  private ScmContainerLocationResponse submitRpcRequest(
      ScmContainerLocationRequest wrapper) throws ServiceException {
    if (!ADMIN_COMMAND_TYPE.contains(wrapper.getCmdType())) {
      return rpcProxy.submitRequest(NULL_RPC_CONTROLLER, wrapper);
    }

    // TODO: Modify ScmContainerLocationResponse to hold results from multi SCM
    ScmContainerLocationResponse response = null;
    for (StorageContainerLocationProtocolPB proxy : fpp.getProxies()) {
      response = proxy.submitRequest(NULL_RPC_CONTROLLER, wrapper);
    }
    return response;
  }

  /**
   * Asks SCM where a container should be allocated. SCM responds with the set
   * of datanodes that should be used creating this container. Ozone/SCM only
   * supports replication factor of either 1 or 3.
   *
   * @param type   - Replication Type
   * @param factor - Replication Count
   * @param owner  - Service owner of the container.
   */
  @Override
  public ContainerWithPipeline allocateContainer(
      HddsProtos.ReplicationType type, HddsProtos.ReplicationFactor factor,
      String owner) throws IOException {
    ReplicationConfig replicationConfig =
        ReplicationConfig.fromProtoTypeAndFactor(type, factor);
    return allocateContainer(replicationConfig, owner);
  }

  @Override
  public ContainerWithPipeline allocateContainer(
      ReplicationConfig replicationConfig, String owner) throws IOException {

    ContainerRequestProto.Builder request = ContainerRequestProto.newBuilder()
          .setTraceID(TracingUtil.exportCurrentSpan())
          .setReplicationType(replicationConfig.getReplicationType())
          .setOwner(owner);

    if (replicationConfig.getReplicationType() == HddsProtos.ReplicationType.EC) {
      HddsProtos.ECReplicationConfig ecProto =
          ((ECReplicationConfig) replicationConfig).toProto();
      request.setEcReplicationConfig(ecProto);
      request.setReplicationFactor(ReplicationFactor.ONE); // Set for backward compatibility, ignored for EC.
    } else {
      request.setReplicationFactor(ReplicationFactor.valueOf(replicationConfig.getReplication()));
    }

    ContainerResponseProto response =
        submitRequest(Type.AllocateContainer,
            builder -> builder.setContainerRequest(request))
            .getContainerResponse();
    //TODO should be migrated to use the top level status structure.
    if (response.getErrorCode() != ContainerResponseProto.Error.success) {
      throw new IOException(response.hasErrorMessage() ?
          response.getErrorMessage() : "Allocate container failed.");
    }
    return ContainerWithPipeline.fromProtobuf(response.getContainerWithPipeline());
  }

  @Override
  public ContainerInfo getContainer(long containerID) throws IOException {
    Preconditions.checkState(containerID >= 0,
        "Container ID cannot be negative");
    GetContainerRequestProto request = GetContainerRequestProto
        .newBuilder()
        .setContainerID(containerID)
        .setTraceID(TracingUtil.exportCurrentSpan())
        .build();
    ScmContainerLocationResponse response =
        submitRequest(Type.GetContainer,
            (builder) -> builder.setGetContainerRequest(request));
    return ContainerInfo
        .fromProtobuf(response.getGetContainerResponse().getContainerInfo());

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContainerWithPipeline getContainerWithPipeline(long containerID)
      throws IOException {
    Preconditions.checkState(containerID >= 0,
        "Container ID cannot be negative");
    GetContainerWithPipelineRequestProto request =
        GetContainerWithPipelineRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .setContainerID(containerID).build();

    ScmContainerLocationResponse response =
        submitRequest(Type.GetContainerWithPipeline,
            (builder) -> builder.setGetContainerWithPipelineRequest(request));

    return ContainerWithPipeline.fromProtobuf(
        response.getGetContainerWithPipelineResponse()
            .getContainerWithPipeline());

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<HddsProtos.SCMContainerReplicaProto> getContainerReplicas(
      long containerID, int clientVersion) throws IOException {
    Preconditions.checkState(containerID >= 0,
        "Container ID cannot be negative");

    GetContainerReplicasRequestProto request =
        GetContainerReplicasRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .setContainerID(containerID).build();

    ScmContainerLocationResponse response =
        submitRequest(Type.GetContainerReplicas,
            (builder) -> builder.setGetContainerReplicasRequest(request));
    return response.getGetContainerReplicasResponse().getContainerReplicaList();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ContainerWithPipeline> getContainerWithPipelineBatch(
      Iterable<? extends Long> containerIDs) throws IOException {
    for (Long containerID: containerIDs) {
      Preconditions.checkState(containerID >= 0,
          "Container ID cannot be negative");
    }

    GetContainerWithPipelineBatchRequestProto request =
        GetContainerWithPipelineBatchRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .addAllContainerIDs(containerIDs)
            .build();

    ScmContainerLocationResponse response =
        submitRequest(Type.GetContainerWithPipelineBatch,
            (builder) -> builder
                .setGetContainerWithPipelineBatchRequest(request));

    List<HddsProtos.ContainerWithPipeline> protoCps = response
        .getGetContainerWithPipelineBatchResponse()
        .getContainerWithPipelinesList();

    List<ContainerWithPipeline> cps = new ArrayList<>();

    for (HddsProtos.ContainerWithPipeline cp : protoCps) {
      cps.add(ContainerWithPipeline.fromProtobuf(cp));
    }

    return cps;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<ContainerWithPipeline> getExistContainerWithPipelinesInBatch(
      List<Long> containerIDs) {
    for (Long containerID: containerIDs) {
      Preconditions.checkState(containerID >= 0,
          "Container ID cannot be negative");
    }

    GetExistContainerWithPipelinesInBatchRequestProto request =
        GetExistContainerWithPipelinesInBatchRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .addAllContainerIDs(containerIDs)
            .build();
    ScmContainerLocationResponse response = null;
    List<ContainerWithPipeline> cps = new ArrayList<>();
    try {
      response = submitRequest(Type.GetExistContainerWithPipelinesInBatch,
          (builder) -> builder
              .setGetExistContainerWithPipelinesInBatchRequest(request));
    } catch (IOException ex) {
      return cps;
    }

    List<HddsProtos.ContainerWithPipeline> protoCps = response
        .getGetExistContainerWithPipelinesInBatchResponse()
        .getContainerWithPipelinesList();

    for (HddsProtos.ContainerWithPipeline cp : protoCps) {
      cps.add(ContainerWithPipeline.fromProtobuf(cp));
    }
    return cps;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ContainerListResult listContainer(long startContainerID, int count)
      throws IOException {
    return listContainer(startContainerID, count, null, null, null);
  }

  @Override
  public ContainerListResult listContainer(long startContainerID, int count,
      HddsProtos.LifeCycleState state) throws IOException {
    return listContainer(startContainerID, count, state, null, null);
  }

  @Override
  public ContainerListResult listContainer(long startContainerID, int count,
      HddsProtos.LifeCycleState state,
      HddsProtos.ReplicationType replicationType,
      ReplicationConfig replicationConfig)
      throws IOException {
    Preconditions.checkState(startContainerID >= 0,
        "Container ID cannot be negative.");
    Preconditions.checkState(count > 0,
        "Container count must be greater than 0.");
    SCMListContainerRequestProto.Builder builder = SCMListContainerRequestProto
        .newBuilder();
    builder.setStartContainerID(startContainerID);
    builder.setCount(count);
    builder.setTraceID(TracingUtil.exportCurrentSpan());
    if (state != null) {
      builder.setState(state);
    }
    if (replicationConfig != null) {
      if (replicationConfig.getReplicationType() == EC) {
        builder.setType(EC);
        builder.setEcReplicationConfig(
            ((ECReplicationConfig)replicationConfig).toProto());
      } else {
        builder.setType(replicationConfig.getReplicationType());
        builder.setFactor(((ReplicatedReplicationConfig)replicationConfig)
            .getReplicationFactor());
      }
    } else if (replicationType != null) {
      builder.setType(replicationType);
    }

    SCMListContainerRequestProto request = builder.build();

    SCMListContainerResponseProto response =
        submitRequest(Type.ListContainer,
            builder1 -> builder1.setScmListContainerRequest(request))
            .getScmListContainerResponse();
    List<ContainerInfo> containerList = new ArrayList<>();
    for (HddsProtos.ContainerInfoProto containerInfoProto : response
        .getContainersList()) {
      containerList.add(ContainerInfo.fromProtobuf(containerInfoProto));
    }

    if (response.hasContainerCount()) {
      return new ContainerListResult(containerList, response.getContainerCount());
    } else {
      return new ContainerListResult(containerList, -1);
    }
  }

  @Deprecated
  @Override
  public ContainerListResult listContainer(long startContainerID, int count,
      HddsProtos.LifeCycleState state, HddsProtos.ReplicationFactor factor)
      throws IOException {
    throw new UnsupportedOperationException("Should no longer be called from " +
        "the client side");
  }

  /**
   * Ask SCM to delete a container by name. SCM will remove
   * the container mapping in its database.
   */
  @Override
  public void deleteContainer(long containerID)
      throws IOException {
    Preconditions.checkState(containerID >= 0,
        "Container ID cannot be negative");
    SCMDeleteContainerRequestProto request = SCMDeleteContainerRequestProto
        .newBuilder()
        .setTraceID(TracingUtil.exportCurrentSpan())
        .setContainerID(containerID)
        .build();
    submitRequest(Type.DeleteContainer,
        builder -> builder.setScmDeleteContainerRequest(request));

  }

  @Override
  public Map<String, List<ContainerID>> getContainersOnDecomNode(DatanodeDetails dn) throws IOException {
    GetContainersOnDecomNodeRequestProto request = GetContainersOnDecomNodeRequestProto.newBuilder()
        .setDatanodeDetails(dn.getProtoBufMessage()).build();
    GetContainersOnDecomNodeResponseProto response = submitRequest(Type.GetContainersOnDecomNode,
        builder -> builder.setGetContainersOnDecomNodeRequest(request)).getGetContainersOnDecomNodeResponse();
    Map<String, List<ContainerID>> containerMap = new HashMap<>();
    for (ContainersOnDecomNodeProto containersProto : response.getContainersOnDecomNodeList()) {
      List<ContainerID> containerIds = new ArrayList<>();
      for (HddsProtos.ContainerID id : containersProto.getIdList()) {
        containerIds.add(ContainerID.getFromProtobuf(id));
      }
      containerMap.put(containersProto.getName(), containerIds);
    }
    return containerMap;
  }

  /**
   * Queries a list of Nodes based on their operational state or health state.
   * Passing a null for either value acts as a wildcard for that state.
   *
   * @param opState The operation state of the node
   * @param nodeState The health of the node
   * @return List of Datanodes.
   */
  @Override
  public List<HddsProtos.Node> queryNode(
      HddsProtos.NodeOperationalState opState, HddsProtos.NodeState
      nodeState, HddsProtos.QueryScope queryScope, String poolName,
      int clientVersion) throws IOException {
    // TODO : We support only cluster wide query right now. So ignoring checking
    // queryScope and poolName
    NodeQueryRequestProto.Builder builder = NodeQueryRequestProto.newBuilder()
        .setTraceID(TracingUtil.exportCurrentSpan())
        .setScope(queryScope).setPoolName(poolName);
    if (opState != null) {
      builder.setOpState(opState);
    }
    if (nodeState != null) {
      builder.setState(nodeState);
    }
    NodeQueryRequestProto request = builder.build();
    NodeQueryResponseProto response = submitRequest(Type.QueryNode,
        builder1 -> builder1.setNodeQueryRequest(request))
        .getNodeQueryResponse();
    return response.getDatanodesList();
  }

  @Override
  public HddsProtos.Node queryNode(UUID uuid) throws IOException {
    SingleNodeQueryRequestProto request = SingleNodeQueryRequestProto.newBuilder()
        .setUuid(ProtobufUtils.toProtobuf(uuid))
        .build();
    SingleNodeQueryResponseProto response =
        submitRequest(Type.SingleNodeQuery,
            builder -> builder.setSingleNodeQueryRequest(request))
            .getSingleNodeQueryResponse();
    return response.getDatanode();
  }

  /**
   * Attempts to decommission the list of nodes.
   * @param nodes The list of hostnames or hostname:ports to decommission
   * @param force true to skip fail-early checks and try to decommission nodes
   * @throws IOException
   */
  @Override
  public List<DatanodeAdminError> decommissionNodes(List<String> nodes, boolean force)
      throws IOException {
    Objects.requireNonNull(nodes, "nodes == null");
    DecommissionNodesRequestProto request =
        DecommissionNodesRequestProto.newBuilder()
        .addAllHosts(nodes).setForce(force)
        .build();
    DecommissionNodesResponseProto response =
        submitRequest(Type.DecommissionNodes,
            builder -> builder.setDecommissionNodesRequest(request))
            .getDecommissionNodesResponse();
    List<DatanodeAdminError> errors = new ArrayList<>();
    for (DatanodeAdminErrorResponseProto e : response.getFailedHostsList()) {
      errors.add(new DatanodeAdminError(e.getHost(), e.getError()));
    }
    return errors;
  }

  /**
   * Attempts to recommission the list of nodes.
   * @param nodes The list of hostnames or hostname:ports to recommission
   * @throws IOException
   */
  @Override
  public List<DatanodeAdminError> recommissionNodes(List<String> nodes)
      throws IOException {
    Objects.requireNonNull(nodes, "nodes == null");
    RecommissionNodesRequestProto request =
        RecommissionNodesRequestProto.newBuilder()
            .addAllHosts(nodes)
            .build();
    RecommissionNodesResponseProto response =
        submitRequest(Type.RecommissionNodes,
            builder -> builder.setRecommissionNodesRequest(request))
                .getRecommissionNodesResponse();
    List<DatanodeAdminError> errors = new ArrayList<>();
    for (DatanodeAdminErrorResponseProto e : response.getFailedHostsList()) {
      errors.add(new DatanodeAdminError(e.getHost(), e.getError()));
    }
    return errors;
  }

  /**
   * Attempts to put the list of nodes into maintenance mode.
   *
   * @param nodes The list of hostnames or hostname:ports to put into
   *              maintenance
   * @param endInHours A number of hours from now where the nodes will be taken
   *                   out of maintenance automatically. Passing zero will
   *                   allow the nodes to stay in maintenance indefinitely
   * @throws IOException
   */
  @Override
  public List<DatanodeAdminError> startMaintenanceNodes(
      List<String> nodes, int endInHours, boolean force) throws IOException {
    Objects.requireNonNull(nodes, "nodes == null");
    StartMaintenanceNodesRequestProto request =
        StartMaintenanceNodesRequestProto.newBuilder()
            .addAllHosts(nodes)
            .setEndInHours(endInHours)
            .setForce(force)
            .build();
    StartMaintenanceNodesResponseProto response =
        submitRequest(Type.StartMaintenanceNodes,
            builder -> builder.setStartMaintenanceNodesRequest(request))
                .getStartMaintenanceNodesResponse();
    List<DatanodeAdminError> errors = new ArrayList<>();
    for (DatanodeAdminErrorResponseProto e : response.getFailedHostsList()) {
      errors.add(new DatanodeAdminError(e.getHost(), e.getError()));
    }
    return errors;
  }

  /**
   * Close a container.
   *
   * @param containerID ID of the container to close
   * @throws IOException in case of any Exception
   */
  @Override
  public void closeContainer(long containerID) throws IOException {
    Preconditions.checkState(containerID >= 0,
        "Container ID cannot be negative");
    SCMCloseContainerRequestProto request = SCMCloseContainerRequestProto
        .newBuilder()
        .setTraceID(TracingUtil.exportCurrentSpan())
        .setContainerID(containerID)
        .build();
    SCMCloseContainerResponseProto response = submitRequest(Type.CloseContainer,
          builder -> builder.setScmCloseContainerRequest(
            request)).getScmCloseContainerResponse();
    if (response.hasStatus() && (response.getStatus()
        .equals(CONTAINER_ALREADY_CLOSED) || response.getStatus()
        .equals(CONTAINER_ALREADY_CLOSING))) {
      String errorMessage =
          response.getStatus().equals(CONTAINER_ALREADY_CLOSED) ?
              String.format("Container %s already closed", containerID) :
              String.format("Container %s is in closing state", containerID);
      throw new IOException(errorMessage);
    }
  }

  /**
   * Creates a replication pipeline of a specified type.
   *
   * @param replicationType - replication type
   * @param factor          - factor 1 or 3
   * @param nodePool        - optional machine list to build a pipeline.
   */
  @Override
  public Pipeline createReplicationPipeline(HddsProtos.ReplicationType
      replicationType, HddsProtos.ReplicationFactor factor, HddsProtos
      .NodePool nodePool) throws IOException {
    PipelineRequestProto request = PipelineRequestProto.newBuilder()
        .setTraceID(TracingUtil.exportCurrentSpan())
        .setNodePool(nodePool)
        .setReplicationFactor(factor)
        .setReplicationType(replicationType)
        .build();

    PipelineResponseProto response =
        submitRequest(Type.AllocatePipeline,
            builder -> builder.setPipelineRequest(request))
            .getPipelineResponse();
    if (response.getErrorCode() ==
        PipelineResponseProto.Error.success) {
      Preconditions.checkState(response.hasPipeline(), "With success, " +
          "must come a pipeline");
      return Pipeline.getFromProtobuf(response.getPipeline());
    } else {
      String errorMessage = String.format("create replication pipeline " +
              "failed. code : %s Message: %s", response.getErrorCode(),
          response.hasErrorMessage() ? response.getErrorMessage() : "");
      throw new IOException(errorMessage);
    }

  }

  @Override
  public List<Pipeline> listPipelines() throws IOException {
    ListPipelineRequestProto request = ListPipelineRequestProto
        .newBuilder().setTraceID(TracingUtil.exportCurrentSpan())
        .build();

    ListPipelineResponseProto response = submitRequest(Type.ListPipelines,
        builder -> builder.setListPipelineRequest(request))
        .getListPipelineResponse();

    List<Pipeline> list = new ArrayList<>();
    for (HddsProtos.Pipeline pipeline : response.getPipelinesList()) {
      Pipeline fromProtobuf = Pipeline.getFromProtobuf(pipeline);
      list.add(fromProtobuf);
    }
    return list;

  }

  @Override
  public Pipeline getPipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    GetPipelineRequestProto request = GetPipelineRequestProto.newBuilder()
            .setPipelineID(pipelineID)
            .setTraceID(TracingUtil.exportCurrentSpan())
            .build();
    GetPipelineResponseProto response = submitRequest(Type.GetPipeline,
        builder -> builder.setGetPipelineRequest(request))
        .getGetPipelineResponse();

    return Pipeline.getFromProtobuf(response.getPipeline());
  }

  @Override
  public void activatePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {
    ActivatePipelineRequestProto request =
        ActivatePipelineRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .setPipelineID(pipelineID)
            .build();
    submitRequest(Type.ActivatePipeline,
        builder -> builder.setActivatePipelineRequest(request));

  }

  @Override
  public void deactivatePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {

    DeactivatePipelineRequestProto request =
        DeactivatePipelineRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .setPipelineID(pipelineID)
            .build();
    submitRequest(Type.DeactivatePipeline,
        builder -> builder.setDeactivatePipelineRequest(request));
  }

  @Override
  public void closePipeline(HddsProtos.PipelineID pipelineID)
      throws IOException {

    ClosePipelineRequestProto request =
        ClosePipelineRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .setPipelineID(pipelineID)
            .build();
    submitRequest(Type.ClosePipeline,
        builder -> builder.setClosePipelineRequest(request));

  }

  @Override
  public ScmInfo getScmInfo() throws IOException {
    HddsProtos.GetScmInfoRequestProto request =
        HddsProtos.GetScmInfoRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .build();

    GetScmInfoResponseProto resp = submitRequest(Type.GetScmInfo,
        builder -> builder.setGetScmInfoRequest(request))
        .getGetScmInfoResponse();
    ScmInfo.Builder builder = new ScmInfo.Builder()
        .setClusterId(resp.getClusterId())
        .setScmId(resp.getScmId())
        .setPeerRoles(resp.getPeerRolesList());
    return builder.build();
  }

  @Override
  public void transferLeadership(String nodeId)
      throws IOException {
    TransferLeadershipRequestProto.Builder reqBuilder =
        TransferLeadershipRequestProto.newBuilder();
    reqBuilder.setNewLeaderId(nodeId);
    submitRequest(Type.TransferLeadership,
        builder -> builder.setTransferScmLeadershipRequest(reqBuilder.build()));
  }

  @Deprecated
  @Override
  public List<DeletedBlocksTransactionInfo> getFailedDeletedBlockTxn(int count,
      long startTxId) throws IOException {
    return Collections.emptyList();
  }

  @Deprecated
  @Override
  public int resetDeletedBlockRetryCount(List<Long> txIDs)
      throws IOException {
    return 0;
  }

  @Nullable
  @Override
  public DeletedBlocksTransactionSummary getDeletedBlockSummary() throws IOException {
    GetDeletedBlocksTxnSummaryRequestProto request =
        GetDeletedBlocksTxnSummaryRequestProto.newBuilder().build();
    ScmContainerLocationResponse scmContainerLocationResponse = submitRequest(Type.GetDeletedBlocksTransactionSummary,
        builder -> builder.setGetDeletedBlocksTxnSummaryRequest(request));
    GetDeletedBlocksTxnSummaryResponseProto response =
        scmContainerLocationResponse.getGetDeletedBlocksTxnSummaryResponse();
    return response.hasSummary() ? response.getSummary() : null;
  }

  /**
   * Check if SCM is in safe mode.
   *
   * @return Returns true if SCM is in safe mode else returns false.
   */
  @Override
  public boolean inSafeMode() throws IOException {
    InSafeModeRequestProto request =
        InSafeModeRequestProto.getDefaultInstance();

    return submitRequest(Type.InSafeMode,
        builder -> builder.setInSafeModeRequest(request))
        .getInSafeModeResponse().getInSafeMode();

  }

  @Override
  public Map<String, Pair<Boolean, String>> getSafeModeRuleStatuses()
      throws IOException {
    GetSafeModeRuleStatusesRequestProto request =
        GetSafeModeRuleStatusesRequestProto.getDefaultInstance();
    GetSafeModeRuleStatusesResponseProto response =
        submitRequest(Type.GetSafeModeRuleStatuses,
            builder -> builder.setGetSafeModeRuleStatusesRequest(request))
            .getGetSafeModeRuleStatusesResponse();
    Map<String, Pair<Boolean, String>> map = new HashMap();
    for (SafeModeRuleStatusProto statusProto :
        response.getSafeModeRuleStatusesProtoList()) {
      map.put(statusProto.getRuleName(),
          Pair.of(statusProto.getValidate(), statusProto.getStatusText()));
    }
    return map;
  }

  /**
   * Force SCM out of Safe mode.
   *
   * @return returns true if operation is successful.
   */
  @Override
  public boolean forceExitSafeMode() throws IOException {
    ForceExitSafeModeRequestProto request =
        ForceExitSafeModeRequestProto.getDefaultInstance();
    ForceExitSafeModeResponseProto resp =
        submitRequest(Type.ForceExitSafeMode,
            builder -> builder.setForceExitSafeModeRequest(request))
            .getForceExitSafeModeResponse();

    return resp.getExitedSafeMode();

  }

  @Override
  public void startReplicationManager() throws IOException {

    StartReplicationManagerRequestProto request =
        StartReplicationManagerRequestProto.getDefaultInstance();
    submitRequest(Type.StartReplicationManager,
        builder -> builder.setStartReplicationManagerRequest(request));

  }

  @Override
  public void stopReplicationManager() throws IOException {

    StopReplicationManagerRequestProto request =
        StopReplicationManagerRequestProto.getDefaultInstance();
    submitRequest(Type.StopReplicationManager,
        builder -> builder.setStopReplicationManagerRequest(request));

  }

  @Override
  public boolean getReplicationManagerStatus() throws IOException {

    ReplicationManagerStatusRequestProto request =
        ReplicationManagerStatusRequestProto.getDefaultInstance();
    ReplicationManagerStatusResponseProto response =
        submitRequest(Type.GetReplicationManagerStatus,
            builder -> builder.setSeplicationManagerStatusRequest(request))
            .getReplicationManagerStatusResponse();
    return response.getIsRunning();

  }

  @Override
  public ReplicationManagerReport getReplicationManagerReport()
      throws IOException {
    ReplicationManagerReportRequestProto request =
        ReplicationManagerReportRequestProto.newBuilder()
            .setTraceID(TracingUtil.exportCurrentSpan())
            .build();
    ReplicationManagerReportResponseProto response =
        submitRequest(Type.GetReplicationManagerReport,
            builder -> builder.setReplicationManagerReportRequest(request))
        .getGetReplicationManagerReportResponse();
    return ReplicationManagerReport.fromProtobuf(response.getReport());
  }

  @Override
  public StartContainerBalancerResponseProto startContainerBalancer(
      Optional<Double> threshold, Optional<Integer> iterations,
      Optional<Integer> maxDatanodesPercentageToInvolvePerIteration,
      Optional<Long> maxSizeToMovePerIterationInGB,
      Optional<Long> maxSizeEnteringTargetInGB,
      Optional<Long> maxSizeLeavingSourceInGB,
      Optional<Integer> balancingInterval,
      Optional<Integer> moveTimeout,
      Optional<Integer> moveReplicationTimeout,
      Optional<Boolean> networkTopologyEnable,
      Optional<String> includeNodes,
      Optional<String> excludeNodes) throws IOException {
    StartContainerBalancerRequestProto.Builder builder =
        StartContainerBalancerRequestProto.newBuilder();
    builder.setTraceID(TracingUtil.exportCurrentSpan());

    //make balancer configuration optional
    if (threshold.isPresent()) {
      double tsd = threshold.get();
      Preconditions.checkState(tsd >= 0.0D && tsd < 100D,
          "Threshold should be specified in the range [0.0, 100.0).");
      builder.setThreshold(tsd);
    }
    if (maxSizeToMovePerIterationInGB.isPresent()) {
      long mstm = maxSizeToMovePerIterationInGB.get();
      Preconditions.checkState(mstm > 0,
          "Max Size To Move Per Iteration In GB must be positive.");
      builder.setMaxSizeToMovePerIterationInGB(mstm);
    }
    if (maxDatanodesPercentageToInvolvePerIteration.isPresent()) {
      int mdti = maxDatanodesPercentageToInvolvePerIteration.get();
      Preconditions.checkState(mdti >= 0,
          "Max Datanodes Percentage To Involve Per Iteration must be " +
              "greater than equal to zero.");
      Preconditions.checkState(mdti <= 100,
          "Max Datanodes Percentage To Involve Per Iteration must be " +
              "lesser than equal to hundred.");
      builder.setMaxDatanodesPercentageToInvolvePerIteration(mdti);
    }
    if (iterations.isPresent()) {
      int i = iterations.get();
      Preconditions.checkState(i > 0 || i == -1,
          "Number of Iterations must be positive or" +
              " -1 (for running container balancer infinitely).");
      builder.setIterations(i);
    }

    if (maxSizeEnteringTargetInGB.isPresent()) {
      long mset = maxSizeEnteringTargetInGB.get();
      Preconditions.checkState(mset > 0,
          "Max Size Entering Target In GB must be positive.");
      builder.setMaxSizeEnteringTargetInGB(mset);
    }

    if (maxSizeLeavingSourceInGB.isPresent()) {
      long msls = maxSizeLeavingSourceInGB.get();
      Preconditions.checkState(msls > 0,
          "Max Size Leaving Source In GB must be positive.");
      builder.setMaxSizeLeavingSourceInGB(msls);
    }

    if (balancingInterval.isPresent()) {
      int bi = balancingInterval.get();
      Preconditions.checkState(bi > 0,
              "Balancing Interval must be greater than zero.");
      builder.setBalancingInterval(bi);
    }

    if (moveTimeout.isPresent()) {
      int mt = moveTimeout.get();
      Preconditions.checkState(mt > 0,
              "Move Timeout must be greater than zero.");
      builder.setMoveTimeout(mt);
    }

    if (moveReplicationTimeout.isPresent()) {
      int mrt = moveReplicationTimeout.get();
      Preconditions.checkState(mrt > 0,
              "Move Replication Timeout must be greater than zero.");
      builder.setMoveReplicationTimeout(mrt);
    }

    if (networkTopologyEnable.isPresent()) {
      Boolean nt = networkTopologyEnable.get();
      builder.setNetworkTopologyEnable(nt);
    }

    if (includeNodes.isPresent()) {
      String in = includeNodes.get();
      builder.setIncludeNodes(in);
    }

    if (excludeNodes.isPresent()) {
      String ex = excludeNodes.get();
      builder.setExcludeNodes(ex);
    }

    StartContainerBalancerRequestProto request = builder.build();
    return submitRequest(Type.StartContainerBalancer,
        builder1 -> builder1.setStartContainerBalancerRequest(request))
        .getStartContainerBalancerResponse();
  }

  @Override
  public void stopContainerBalancer() throws IOException {

    StopContainerBalancerRequestProto request =
        StopContainerBalancerRequestProto.getDefaultInstance();
    submitRequest(Type.StopContainerBalancer,
        builder -> builder.setStopContainerBalancerRequest(request));

  }

  @Override
  public boolean getContainerBalancerStatus() throws IOException {

    ContainerBalancerStatusRequestProto request =
        ContainerBalancerStatusRequestProto.getDefaultInstance();
    ContainerBalancerStatusResponseProto response =
        submitRequest(Type.GetContainerBalancerStatus,
            builder -> builder.setContainerBalancerStatusRequest(request))
            .getContainerBalancerStatusResponse();
    return response.getIsRunning();

  }

  @Override
  public ContainerBalancerStatusInfoResponseProto getContainerBalancerStatusInfo() throws IOException {

    ContainerBalancerStatusInfoRequestProto request =
            ContainerBalancerStatusInfoRequestProto.getDefaultInstance();
    ContainerBalancerStatusInfoResponseProto response =
            submitRequest(Type.GetContainerBalancerStatusInfo,
                    builder -> builder.setContainerBalancerStatusInfoRequest(request))
                    .getContainerBalancerStatusInfoResponse();
    return response;

  }

  /**
   * Builds request for datanode usage information and receives response.
   *
   * @param address Address String
   * @param uuid UUID String
   * @return List of DatanodeUsageInfoProto. Each element contains info such as
   * capacity, SCMUsed, and remaining space.
   * @throws IOException
   */
  @Override
  public List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(
      String address, String uuid, int clientVersion) throws IOException {

    DatanodeUsageInfoRequestProto request =
        DatanodeUsageInfoRequestProto.newBuilder()
            .setIpaddress(address)
            .setUuid(uuid)
            .build();

    DatanodeUsageInfoResponseProto response =
        submitRequest(Type.DatanodeUsageInfo,
            builder -> builder.setDatanodeUsageInfoRequest(request))
            .getDatanodeUsageInfoResponse();
    return response.getInfoList();
  }

  /**
   * Get usage information of most or least used datanodes.
   *
   * @param mostUsed true if most used, false if least used
   * @param count Integer number of nodes to get info for
   * @return List of DatanodeUsageInfoProto. Each element contains info such as
   * capacity, SCMUsed, and remaining space.
   * @throws IOException
   */
  @Override
  public List<HddsProtos.DatanodeUsageInfoProto> getDatanodeUsageInfo(
      boolean mostUsed, int count, int clientVersion) throws IOException {
    DatanodeUsageInfoRequestProto request =
        DatanodeUsageInfoRequestProto.newBuilder()
            .setMostUsed(mostUsed)
            .setCount(count)
            .build();

    DatanodeUsageInfoResponseProto response =
        submitRequest(Type.DatanodeUsageInfo,
            builder -> builder.setDatanodeUsageInfoRequest(request))
        .getDatanodeUsageInfoResponse();

    return response.getInfoList();
  }

  @Override
  public StatusAndMessages finalizeScmUpgrade(String upgradeClientID)
      throws IOException {
    FinalizeScmUpgradeRequestProto req = FinalizeScmUpgradeRequestProto.
        newBuilder()
        .setUpgradeClientId(upgradeClientID)
        .build();

    FinalizeScmUpgradeResponseProto response =
        submitRequest(Type.FinalizeScmUpgrade,
            builder -> builder.setFinalizeScmUpgradeRequest(req))
            .getFinalizeScmUpgradeResponse();

    UpgradeFinalizationStatus status = response.getStatus();
    return new StatusAndMessages(
        UpgradeFinalization.Status.valueOf(status.getStatus().name()),
        status.getMessagesList());
  }

  @Override
  public StatusAndMessages queryUpgradeFinalizationProgress(
      String upgradeClientID, boolean force, boolean readonly)
      throws IOException {
    QueryUpgradeFinalizationProgressRequestProto req =
        QueryUpgradeFinalizationProgressRequestProto.
            newBuilder()
            .setUpgradeClientId(upgradeClientID)
            .setTakeover(force)
            .setReadonly(readonly)
            .build();

    QueryUpgradeFinalizationProgressResponseProto response =
        submitRequest(Type.QueryUpgradeFinalizationProgress,
            builder -> builder.setQueryUpgradeFinalizationProgressRequest(req))
            .getQueryUpgradeFinalizationProgressResponse();

    UpgradeFinalizationStatus status = response.getStatus();
    return new StatusAndMessages(
        UpgradeFinalization.Status.valueOf(status.getStatus().name()),
        status.getMessagesList());
  }

  @Override
  public Token<?> getContainerToken(
      ContainerID containerID) throws IOException {
    GetContainerTokenRequestProto request =
        GetContainerTokenRequestProto.newBuilder()
            .setContainerID(containerID.getProtobuf())
        .build();

    GetContainerTokenResponseProto response =
        submitRequest(Type.GetContainerToken,
            builder -> builder.setContainerTokenRequest(request))
        .getContainerTokenResponse();
    return OzonePBHelper.tokenFromProto(response.getToken());
  }

  @Override
  public long getContainerCount() throws IOException {
    GetContainerCountRequestProto request =
        GetContainerCountRequestProto.newBuilder().build();

    GetContainerCountResponseProto response =
        submitRequest(Type.GetContainerCount,
          builder -> builder.setGetContainerCountRequest(request))
        .getGetContainerCountResponse();
    return response.getContainerCount();
  }

  @Override
  public long getContainerCount(HddsProtos.LifeCycleState state)
      throws IOException {
    GetContainerCountRequestProto request =
        GetContainerCountRequestProto.newBuilder().build();

    GetContainerCountResponseProto response =
        submitRequest(Type.GetClosedContainerCount,
            builder -> builder.setGetContainerCountRequest(request))
            .getGetContainerCountResponse();
    return response.getContainerCount();
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public List<ContainerInfo> getListOfContainers(
      long startContainerID, int count, HddsProtos.LifeCycleState state)
      throws IOException {
    return listContainer(startContainerID, count, state).getContainerInfoList();
  }

  @Override
  public DecommissionScmResponseProto decommissionScm(
      String scmId) throws IOException {

    DecommissionScmRequestProto request = DecommissionScmRequestProto
        .newBuilder()
        .setScmId(scmId)
        .build();
    DecommissionScmResponseProto response =
        submitRequest(Type.DecommissionScm,
            builder -> builder.setDecommissionScmRequest(request))
                .getDecommissionScmResponse();
    return response;
  }

  @Override
  public String getMetrics(String query) throws IOException {
    GetMetricsRequestProto request = GetMetricsRequestProto.newBuilder().setQuery(query).build();
    GetMetricsResponseProto response = submitRequest(Type.GetMetrics,
        builder -> builder.setGetMetricsRequest(request)).getGetMetricsResponse();
    String metricsJsonStr = response.getMetricsJson();
    return metricsJsonStr;
  }

  @Override
  public void reconcileContainer(long containerID) throws IOException {
    ReconcileContainerRequestProto request = ReconcileContainerRequestProto.newBuilder()
        .setContainerID(containerID)
        .build();
    // TODO check error handling.
    submitRequest(Type.ReconcileContainer, builder -> builder.setReconcileContainerRequest(request));
  }
}
