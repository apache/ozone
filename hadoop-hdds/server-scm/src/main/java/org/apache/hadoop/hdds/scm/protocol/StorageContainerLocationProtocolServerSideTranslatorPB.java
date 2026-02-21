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

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.PipelineResponseProto.Error.errorPipelineAlreadyExists;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.PipelineResponseProto.Error.success;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMCloseContainerResponseProto.Status.CONTAINER_ALREADY_CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMCloseContainerResponseProto.Status.CONTAINER_ALREADY_CLOSING;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.Type.GetContainer;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.Type.GetContainerWithPipeline;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.Type.GetContainerWithPipelineBatch;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.Type.GetExistContainerWithPipelinesInBatch;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.Type.GetPipeline;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.Type.ListContainer;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.Type.ListPipelines;
import static org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol.ADMIN_COMMAND_TYPE;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.TransferLeadershipRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.TransferLeadershipResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.UpgradeFinalizationStatus;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ActivatePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ActivatePipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ClosePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ClosePipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerBalancerStatusResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainersOnDecomNodeProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DatanodeAdminErrorResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DatanodeUsageInfoResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DeactivatePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DeactivatePipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionNodesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionNodesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.FinalizeScmUpgradeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.FinalizeScmUpgradeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ForceExitSafeModeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ForceExitSafeModeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerCountResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerReplicasRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerReplicasResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerTokenRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerTokenResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineBatchRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineBatchResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainersOnDecomNodeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainersOnDecomNodeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetDeletedBlocksTxnSummaryRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetDeletedBlocksTxnSummaryResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetExistContainerWithPipelinesInBatchRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetExistContainerWithPipelinesInBatchResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetFailedDeletedBlocksTxnRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetFailedDeletedBlocksTxnResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetMetricsRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetMetricsResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetPipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetSafeModeRuleStatusesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetSafeModeRuleStatusesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.InSafeModeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.InSafeModeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ListPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ListPipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.NodeQueryResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.PipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.QueryUpgradeFinalizationProgressRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.QueryUpgradeFinalizationProgressResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.RecommissionNodesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.RecommissionNodesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReconcileContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReconcileContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReplicationManagerReportRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReplicationManagerReportResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReplicationManagerStatusRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReplicationManagerStatusResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ResetDeletedBlockRetryCountRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ResetDeletedBlockRetryCountResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMCloseContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMCloseContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMDeleteContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMDeleteContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMListContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMListContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SafeModeRuleStatusProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationRequest;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationResponse;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationResponse.Status;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SingleNodeQueryRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SingleNodeQueryResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartContainerBalancerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartContainerBalancerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartMaintenanceNodesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartMaintenanceNodesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartReplicationManagerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartReplicationManagerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StopContainerBalancerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StopContainerBalancerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StopReplicationManagerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StopReplicationManagerResponseProto;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerListResult;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.RatisUtil;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.OzonePBHelper;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link StorageContainerLocationProtocolPB} to the
 * {@link StorageContainerLocationProtocol} server implementation.
 */
@InterfaceAudience.Private
@SuppressWarnings({"method"})
public final class StorageContainerLocationProtocolServerSideTranslatorPB
    implements StorageContainerLocationProtocolPB {

  private static final Logger LOG =
      LoggerFactory.getLogger(
          StorageContainerLocationProtocolServerSideTranslatorPB.class);
  private static final String ERROR_LIST_CONTAINS_EC_REPLICATION_CONFIG =
      "The returned list of containers contains containers with Erasure Coded"
          + " replication type, which the client won't be able to understand."
          + " Please upgrade the client to a version that supports Erasure"
          + " Coded data, and retry!";
  private static final String ERROR_RESPONSE_CONTAINS_EC_REPLICATION_CONFIG =
      "The returned container data contains Erasure Coded replication"
          + " information, which the client won't be able to understand."
          + " Please upgrade the client to a version that supports Erasure"
          + " Coded data, and retry!";

  private final StorageContainerLocationProtocol impl;
  private final StorageContainerManager scm;
  private static final String ROLE_TYPE = "SCM";

  private OzoneProtocolMessageDispatcher<ScmContainerLocationRequest,
      ScmContainerLocationResponse, StorageContainerLocationProtocolProtos.Type>
      dispatcher;

  /**
   * Creates a new StorageContainerLocationProtocolServerSideTranslatorPB.
   *
   * @param impl            {@link StorageContainerLocationProtocol} server
   *                        implementation
   * @param protocolMetrics
   */
  public StorageContainerLocationProtocolServerSideTranslatorPB(
      StorageContainerLocationProtocol impl,
      StorageContainerManager scm,
      ProtocolMessageMetrics<StorageContainerLocationProtocolProtos.Type> protocolMetrics)
      throws IOException {
    this.impl = impl;
    this.scm = scm;
    this.dispatcher =
        new OzoneProtocolMessageDispatcher<>("ScmContainerLocation",
            protocolMetrics, LOG);
  }

  @Override
  public ScmContainerLocationResponse submitRequest(RpcController controller,
      ScmContainerLocationRequest request) throws ServiceException {
    // not leader or not belong to admin command.
    if (!scm.checkLeader()
        && !ADMIN_COMMAND_TYPE.contains(request.getCmdType())) {
      RatisUtil.checkRatisException(
          scm.getScmHAManager().getRatisServer().triggerNotLeaderException(),
          scm.getClientRpcPort(), scm.getScmId(), scm.getHostname(), ROLE_TYPE);
    }
    // After the request interceptor (now validator) framework is extended to
    // this server interface, this should be removed and solved via new
    // annotated interceptors.
    boolean checkResponseForECRepConfig = false;
    if (request.getVersion() <
        ClientVersion.ERASURE_CODING_SUPPORT.toProtoValue()) {
      if (request.getCmdType() == GetContainer
          || request.getCmdType() == ListContainer
          || request.getCmdType() == GetContainerWithPipeline
          || request.getCmdType() == GetContainerWithPipelineBatch
          || request.getCmdType() == GetExistContainerWithPipelinesInBatch
          || request.getCmdType() == ListPipelines
          || request.getCmdType() == GetPipeline) {

        checkResponseForECRepConfig = true;
      }
    }
    ScmContainerLocationResponse response = dispatcher
        .processRequest(request, this::processRequest, request.getCmdType(),
            request.getTraceID());
    if (checkResponseForECRepConfig) {
      try {
        switch (response.getCmdType()) {
        case GetContainer:
          disallowECReplicationConfigInGetContainerResponse(response);
          break;
        case ListContainer:
          disallowECReplicationConfigInListContainerResponse(response);
          break;
        case GetContainerWithPipeline:
          disallowECReplicationConfigInGetContainerWithPipelineResponse(
              response);
          break;
        case GetContainerWithPipelineBatch:
          disallowECReplicationConfigInGetContainerWithPipelineBatchResponse(
              response);
          break;
        case GetExistContainerWithPipelinesInBatch:
          disallowECReplicationConfigInGetExistContainerWithPipelineBatchResp(
              response);
          break;
        case ListPipelines:
          disallowECReplicationConfigInListPipelinesResponse(response);
          break;
        case GetPipeline:
          disallowECReplicationConfigInGetPipelineResponse(response);
          break;
        default:
        }
      } catch (SCMException e) {
        throw new ServiceException(e);
      }
    }
    return response;
  }

  private void disallowECReplicationConfigInListContainerResponse(
      ScmContainerLocationResponse response) throws SCMException {
    if (!response.hasScmListContainerResponse()) {
      return;
    }
    for (HddsProtos.ContainerInfoProto containerInfo :
        response.getScmListContainerResponse().getContainersList()) {
      if (containerInfo.hasEcReplicationConfig()) {
        throw new SCMException(ERROR_LIST_CONTAINS_EC_REPLICATION_CONFIG,
            SCMException.ResultCodes.INTERNAL_ERROR);
      }
    }
  }

  private void disallowECReplicationConfigInGetContainerResponse(
      ScmContainerLocationResponse response) throws SCMException {
    if (!response.hasGetContainerResponse()) {
      return;
    }
    if (!response.getGetContainerResponse().hasContainerInfo()) {
      return;
    }
    if (response.getGetContainerResponse().getContainerInfo()
        .hasEcReplicationConfig()) {
      throw new SCMException(ERROR_RESPONSE_CONTAINS_EC_REPLICATION_CONFIG,
          SCMException.ResultCodes.INTERNAL_ERROR);
    }
  }

  private void disallowECReplicationConfigInGetContainerWithPipelineResponse(
      ScmContainerLocationResponse response) throws SCMException {
    if (!response.hasGetContainerWithPipelineResponse()) {
      return;
    }
    if (!response.getGetContainerWithPipelineResponse()
        .hasContainerWithPipeline()) {
      return;
    }
    if (response.getGetContainerWithPipelineResponse()
        .getContainerWithPipeline().hasContainerInfo()) {
      HddsProtos.ContainerInfoProto containerInfo =
          response.getGetContainerWithPipelineResponse()
              .getContainerWithPipeline().getContainerInfo();
      if (containerInfo.hasEcReplicationConfig()) {
        throw new SCMException(ERROR_RESPONSE_CONTAINS_EC_REPLICATION_CONFIG,
            SCMException.ResultCodes.INTERNAL_ERROR);
      }
    }
    if (response.getGetContainerWithPipelineResponse()
        .getContainerWithPipeline().hasPipeline()) {
      HddsProtos.Pipeline pipeline =
          response.getGetContainerWithPipelineResponse()
              .getContainerWithPipeline().getPipeline();
      if (pipeline.hasEcReplicationConfig()) {
        throw new SCMException(ERROR_RESPONSE_CONTAINS_EC_REPLICATION_CONFIG,
            SCMException.ResultCodes.INTERNAL_ERROR);
      }
    }
  }

  private void
      disallowECReplicationConfigInGetContainerWithPipelineBatchResponse(
      ScmContainerLocationResponse response) throws SCMException {
    if (!response.hasGetContainerWithPipelineBatchResponse()) {
      return;
    }
    List<HddsProtos.ContainerWithPipeline> cwps =
        response.getGetContainerWithPipelineBatchResponse()
            .getContainerWithPipelinesList();
    checkForECReplicationConfigIn(cwps);
  }

  private void
      disallowECReplicationConfigInGetExistContainerWithPipelineBatchResp(
      ScmContainerLocationResponse response) throws SCMException {
    if (!response.hasGetExistContainerWithPipelinesInBatchResponse()) {
      return;
    }
    List<HddsProtos.ContainerWithPipeline> cwps =
        response.getGetExistContainerWithPipelinesInBatchResponse()
            .getContainerWithPipelinesList();
    checkForECReplicationConfigIn(cwps);
  }

  private void checkForECReplicationConfigIn(
      List<HddsProtos.ContainerWithPipeline> cwps)
      throws SCMException {
    for (HddsProtos.ContainerWithPipeline cwp : cwps) {
      if (cwp.hasContainerInfo()) {
        if (cwp.getContainerInfo().hasEcReplicationConfig()) {
          throw new SCMException(ERROR_LIST_CONTAINS_EC_REPLICATION_CONFIG,
              SCMException.ResultCodes.INTERNAL_ERROR);
        }
      }
      if (cwp.hasPipeline()) {
        if (cwp.getPipeline().hasEcReplicationConfig()) {
          throw new SCMException(ERROR_LIST_CONTAINS_EC_REPLICATION_CONFIG,
              SCMException.ResultCodes.INTERNAL_ERROR);
        }
      }
    }
  }

  private void disallowECReplicationConfigInListPipelinesResponse(
      ScmContainerLocationResponse response) throws SCMException {
    if (!response.hasListPipelineResponse()) {
      return;
    }
    for (HddsProtos.Pipeline pipeline :
        response.getListPipelineResponse().getPipelinesList()) {
      if (pipeline.hasEcReplicationConfig()) {
        throw new SCMException("The returned list of pipelines contains"
            + " pipelines with Erasure Coded replication type, which the"
            + " client won't be able to understand."
            + " Please upgrade the client to a version that supports Erasure"
            + " Coded data, and retry!",
            SCMException.ResultCodes.INTERNAL_ERROR);
      }
    }
  }

  private void disallowECReplicationConfigInGetPipelineResponse(
      ScmContainerLocationResponse response) throws SCMException {
    if (!response.hasGetPipelineResponse()) {
      return;
    }
    if (response.getPipelineResponse().getPipeline().hasEcReplicationConfig()) {
      throw new SCMException("The returned pipeline data contains"
          + " Erasure Coded replication information, which the client won't"
          + " be able to understand."
          + " Please upgrade the client to a version that supports Erasure"
          + " Coded data, and retry!",
          SCMException.ResultCodes.INTERNAL_ERROR);
    }
  }

  @SuppressWarnings("checkstyle:methodlength")
  public ScmContainerLocationResponse processRequest(
      ScmContainerLocationRequest request) throws ServiceException {
    try {
      switch (request.getCmdType()) {
      case AllocateContainer:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setContainerResponse(allocateContainer(
                request.getContainerRequest(), request.getVersion()))
            .build();
      case GetContainer:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetContainerResponse(
                getContainer(request.getGetContainerRequest()))
            .build();
      case GetContainerToken:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setContainerTokenResponse(
                getContainerToken(request.getContainerTokenRequest()))
            .build();
      case GetContainerWithPipeline:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetContainerWithPipelineResponse(getContainerWithPipeline(
                request.getGetContainerWithPipelineRequest(),
                request.getVersion()))
            .build();
      case GetContainerWithPipelineBatch:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetContainerWithPipelineBatchResponse(
                getContainerWithPipelineBatch(
                    request.getGetContainerWithPipelineBatchRequest(),
                    request.getVersion()))
            .build();
      case GetExistContainerWithPipelinesInBatch:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetExistContainerWithPipelinesInBatchResponse(
                getExistContainerWithPipelinesInBatch(
                    request.getGetExistContainerWithPipelinesInBatchRequest(),
                    request.getVersion()))
            .build();
      case ListContainer:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setScmListContainerResponse(listContainer(
                request.getScmListContainerRequest()))
            .build();
      case QueryNode:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setNodeQueryResponse(queryNode(request.getNodeQueryRequest(),
                request.getVersion()))
            .build();
      case SingleNodeQuery:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setSingleNodeQueryResponse(querySingleNode(request
                .getSingleNodeQueryRequest()))
            .build();
      case CloseContainer:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setScmCloseContainerResponse(closeContainer(
                request.getScmCloseContainerRequest()))
            .build();
      case AllocatePipeline:
        if (scm.getLayoutVersionManager().needsFinalization() &&
            !scm.getLayoutVersionManager().isAllowed(
                HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)
        ) {
          if (request.getPipelineRequest().getReplicationType() ==
              HddsProtos.ReplicationType.EC) {
            throw new SCMException("Cluster is not finalized yet, it is"
                + " not enabled to create pipelines with Erasure Coded"
                + " replication type.",
                SCMException.ResultCodes.INTERNAL_ERROR);
          }
        }
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setPipelineResponse(allocatePipeline(
                request.getPipelineRequest(), request.getVersion()))
            .build();
      case ListPipelines:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setListPipelineResponse(listPipelines(
                request.getListPipelineRequest(), request.getVersion()))
            .build();
      case ActivatePipeline:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setActivatePipelineResponse(activatePipeline(
                request.getActivatePipelineRequest()))
            .build();
      case DeactivatePipeline:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setDeactivatePipelineResponse(deactivatePipeline(
                request.getDeactivatePipelineRequest()))
            .build();
      case ClosePipeline:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setClosePipelineResponse(closePipeline(
                request.getClosePipelineRequest()))
            .build();
      case GetScmInfo:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetScmInfoResponse(getScmInfo(
                request.getGetScmInfoRequest()))
            .build();
      case InSafeMode:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setInSafeModeResponse(inSafeMode(
                request.getInSafeModeRequest()))
            .build();
      case ForceExitSafeMode:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setForceExitSafeModeResponse(forceExitSafeMode(
                request.getForceExitSafeModeRequest()))
            .build();
      case StartReplicationManager:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setStartReplicationManagerResponse(startReplicationManager(
                request.getStartReplicationManagerRequest()))
            .build();
      case StopReplicationManager:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setStopReplicationManagerResponse(stopReplicationManager(
                request.getStopReplicationManagerRequest()))
            .build();
      case GetReplicationManagerStatus:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setReplicationManagerStatusResponse(getReplicationManagerStatus(
                request.getSeplicationManagerStatusRequest()))
            .build();
      case GetReplicationManagerReport:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetReplicationManagerReportResponse(getReplicationManagerReport(
                request.getReplicationManagerReportRequest()))
            .build();
      case StartContainerBalancer:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setStartContainerBalancerResponse(startContainerBalancer(
                request.getStartContainerBalancerRequest()))
            .build();
      case StopContainerBalancer:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setStopContainerBalancerResponse(stopContainerBalancer(
                request.getStopContainerBalancerRequest()))
            .build();
      case GetContainerBalancerStatus:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setContainerBalancerStatusResponse(getContainerBalancerStatus(
                request.getContainerBalancerStatusRequest()))
            .build();
      case GetContainerBalancerStatusInfo:
        return ScmContainerLocationResponse.newBuilder()
                .setCmdType(request.getCmdType())
                .setStatus(Status.OK)
                .setContainerBalancerStatusInfoResponse(getContainerBalancerStatusInfo(
                        request.getContainerBalancerStatusInfoRequest()))
                .build();
      case GetPipeline:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetPipelineResponse(getPipeline(
                request.getGetPipelineRequest(), request.getVersion()))
            .build();
      case GetSafeModeRuleStatuses:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType()).setStatus(Status.OK)
            .setGetSafeModeRuleStatusesResponse(getSafeModeRuleStatues(
                request.getGetSafeModeRuleStatusesRequest()))
            .build();
      case DecommissionNodes:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setDecommissionNodesResponse(decommissionNodes(
                request.getDecommissionNodesRequest()))
            .build();
      case GetContainersOnDecomNode:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetContainersOnDecomNodeResponse(getContainersOnDecomNode(request.getGetContainersOnDecomNodeRequest()))
            .build();
      case RecommissionNodes:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setRecommissionNodesResponse(recommissionNodes(
                request.getRecommissionNodesRequest()))
            .build();
      case StartMaintenanceNodes:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setStartMaintenanceNodesResponse(startMaintenanceNodes(
                request.getStartMaintenanceNodesRequest()))
          .build();
      case FinalizeScmUpgrade:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setFinalizeScmUpgradeResponse(getFinalizeScmUpgrade(
                request.getFinalizeScmUpgradeRequest()))
            .build();
      case QueryUpgradeFinalizationProgress:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setQueryUpgradeFinalizationProgressResponse(
                getQueryUpgradeFinalizationProgress(
                    request.getQueryUpgradeFinalizationProgressRequest()))
            .build();
      case DatanodeUsageInfo:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setDatanodeUsageInfoResponse(getDatanodeUsageInfo(
                request.getDatanodeUsageInfoRequest(),
                request.getVersion()))
            .build();
      case GetContainerCount:
        return ScmContainerLocationResponse.newBuilder()
          .setCmdType(request.getCmdType())
          .setStatus(Status.OK)
          .setGetContainerCountResponse(getContainerCount(
                  request.getGetContainerCountRequest()))
          .build();
      case GetClosedContainerCount:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetContainerCountResponse(getClosedContainerCount(
                request.getGetContainerCountRequest()))
            .build();
      case GetContainerReplicas:
        return ScmContainerLocationResponse.newBuilder()
          .setCmdType(request.getCmdType())
          .setStatus(Status.OK)
          .setGetContainerReplicasResponse(getContainerReplicas(
              request.getGetContainerReplicasRequest(),
              request.getVersion()))
          .build();
      case GetFailedDeletedBlocksTransaction:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetFailedDeletedBlocksTxnResponse(getFailedDeletedBlocksTxn(
                request.getGetFailedDeletedBlocksTxnRequest()
            ))
            .build();
      case ResetDeletedBlockRetryCount:
        return ScmContainerLocationResponse.newBuilder()
              .setCmdType(request.getCmdType())
              .setStatus(Status.OK)
              .setResetDeletedBlockRetryCountResponse(
                  getResetDeletedBlockRetryCount(
                      request.getResetDeletedBlockRetryCountRequest()))
              .build();
      case GetDeletedBlocksTransactionSummary:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetDeletedBlocksTxnSummaryResponse(
                getDeletedBlocksTxnSummary(
                    request.getGetDeletedBlocksTxnSummaryRequest()))
            .build();
      case TransferLeadership:
        return ScmContainerLocationResponse.newBuilder()
              .setCmdType(request.getCmdType())
              .setStatus(Status.OK)
              .setTransferScmLeadershipResponse(
                  transferScmLeadership(
                      request.getTransferScmLeadershipRequest()))
              .build();
      case DecommissionScm:
        return ScmContainerLocationResponse.newBuilder()
              .setCmdType(request.getCmdType())
              .setStatus(Status.OK)
              .setDecommissionScmResponse(decommissionScm(
                  request.getDecommissionScmRequest()))
              .build();
      case GetMetrics:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setGetMetricsResponse(getMetrics(request.getGetMetricsRequest()))
            .build();
      case ReconcileContainer:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setReconcileContainerResponse(reconcileContainer(request.getReconcileContainerRequest()))
            .build();
      default:
        throw new IllegalArgumentException(
            "Unknown command type: " + request.getCmdType());
      }
    } catch (IOException e) {
      RatisUtil
          .checkRatisException(e, scm.getClientRpcPort(), scm.getScmId(), scm.getHostname(), ROLE_TYPE);
      throw new ServiceException(e);
    }
  }

  public GetContainerReplicasResponseProto getContainerReplicas(
      GetContainerReplicasRequestProto request, int clientVersion)
      throws IOException {
    List<HddsProtos.SCMContainerReplicaProto> replicas
        = impl.getContainerReplicas(request.getContainerID(), clientVersion);
    return GetContainerReplicasResponseProto.newBuilder()
        .addAllContainerReplica(replicas).build();
  }

  public ContainerResponseProto allocateContainer(ContainerRequestProto request,
      int clientVersion) throws IOException {
    ReplicationConfig replicationConfig = ReplicationConfig.fromProto(request.getReplicationType(), 
        request.getReplicationFactor(),
        request.getEcReplicationConfig()
    );
    ContainerWithPipeline cp = impl.allocateContainer(replicationConfig, request.getOwner());
    return ContainerResponseProto.newBuilder()
        .setContainerWithPipeline(cp.getProtobuf(clientVersion))
        .setErrorCode(ContainerResponseProto.Error.success)
        .build();

  }

  public GetContainerResponseProto getContainer(
      GetContainerRequestProto request) throws IOException {
    ContainerInfo container = impl.getContainer(request.getContainerID());
    return GetContainerResponseProto.newBuilder()
        .setContainerInfo(container.getProtobuf())
        .build();
  }

  public GetContainerTokenResponseProto getContainerToken(
      GetContainerTokenRequestProto request) throws IOException {
    ContainerID containerID = ContainerID.getFromProtobuf(
        request.getContainerID());
    HddsProtos.TokenProto token = OzonePBHelper.protoFromToken(
        impl.getContainerToken(containerID));
    return GetContainerTokenResponseProto.newBuilder()
        .setToken(token)
        .build();
  }

  public GetContainerWithPipelineResponseProto getContainerWithPipeline(
      GetContainerWithPipelineRequestProto request,
      int clientVersion) throws IOException {
    ContainerWithPipeline container = impl
        .getContainerWithPipeline(request.getContainerID());
    return GetContainerWithPipelineResponseProto.newBuilder()
        .setContainerWithPipeline(container.getProtobuf(clientVersion))
        .build();
  }

  public GetContainerWithPipelineBatchResponseProto
      getContainerWithPipelineBatch(
      GetContainerWithPipelineBatchRequestProto request,
      int clientVersion) throws IOException {
    List<ContainerWithPipeline> containers = impl
        .getContainerWithPipelineBatch(request.getContainerIDsList());
    GetContainerWithPipelineBatchResponseProto.Builder builder =
        GetContainerWithPipelineBatchResponseProto.newBuilder();
    for (ContainerWithPipeline container : containers) {
      builder.addContainerWithPipelines(container.getProtobuf(clientVersion));
    }
    return builder.build();
  }

  public GetExistContainerWithPipelinesInBatchResponseProto
      getExistContainerWithPipelinesInBatch(
      GetExistContainerWithPipelinesInBatchRequestProto request,
      int clientVersion) throws IOException {
    List<ContainerWithPipeline> containers = impl
        .getExistContainerWithPipelinesInBatch(request.getContainerIDsList());
    GetExistContainerWithPipelinesInBatchResponseProto.Builder builder =
        GetExistContainerWithPipelinesInBatchResponseProto.newBuilder();
    for (ContainerWithPipeline container : containers) {
      builder.addContainerWithPipelines(container.getProtobuf(clientVersion));
    }
    return builder.build();
  }

  public SCMListContainerResponseProto listContainer(
      SCMListContainerRequestProto request) throws IOException {

    long startContainerID = 0;
    int count = -1;

    // Arguments check.
    if (request.hasStartContainerID()) {
      // End container name is given.
      startContainerID = request.getStartContainerID();
    }
    count = request.getCount();
    HddsProtos.LifeCycleState state = null;
    HddsProtos.ReplicationFactor factor = null;
    HddsProtos.ReplicationType replicationType = null;
    ReplicationConfig repConfig = null;
    if (request.hasState()) {
      state = request.getState();
    }
    if (request.hasType()) {
      replicationType = request.getType();
    }
    if (replicationType != null) {
      // This must come from an upgraded client as the older version never
      // passed Type. Therefore, we must check for replicationConfig.
      if (replicationType == HddsProtos.ReplicationType.EC) {
        if (request.hasEcReplicationConfig()) {
          repConfig = new ECReplicationConfig(request.getEcReplicationConfig());
        }
      } else {
        if (request.hasFactor()) {
          repConfig = ReplicationConfig
              .fromProtoTypeAndFactor(request.getType(), request.getFactor());
        }
      }
    } else if (request.hasFactor()) {
      factor = request.getFactor();
    }
    ContainerListResult containerListAndTotalCount;
    if (factor != null) {
      // Call from a legacy client
      containerListAndTotalCount =
          impl.listContainer(startContainerID, count, state, factor);
    } else {
      containerListAndTotalCount =
          impl.listContainer(startContainerID, count, state, replicationType, repConfig);
    }
    SCMListContainerResponseProto.Builder builder =
        SCMListContainerResponseProto.newBuilder();
    for (ContainerInfo container : containerListAndTotalCount.getContainerInfoList()) {
      builder.addContainers(container.getProtobuf());
    }
    builder.setContainerCount(containerListAndTotalCount.getTotalCount());
    return builder.build();
  }

  public SCMDeleteContainerResponseProto deleteContainer(
      SCMDeleteContainerRequestProto request)
      throws IOException {
    impl.deleteContainer(request.getContainerID());
    return SCMDeleteContainerResponseProto.newBuilder().build();

  }

  public NodeQueryResponseProto queryNode(
      StorageContainerLocationProtocolProtos.NodeQueryRequestProto request,
      int clientVersion) throws IOException {

    HddsProtos.NodeOperationalState opState = null;
    HddsProtos.NodeState nodeState = null;
    if (request.hasState()) {
      nodeState = request.getState();
    }
    if (request.hasOpState()) {
      opState = request.getOpState();
    }
    List<HddsProtos.Node> datanodes = impl.queryNode(opState, nodeState,
        request.getScope(), request.getPoolName(), clientVersion);
    return NodeQueryResponseProto.newBuilder()
        .addAllDatanodes(datanodes)
        .build();
  }

  public SingleNodeQueryResponseProto querySingleNode(
      SingleNodeQueryRequestProto request)
      throws IOException {

    HddsProtos.Node datanode = impl.queryNode(ProtobufUtils.fromProtobuf(request.getUuid()));
    return SingleNodeQueryResponseProto.newBuilder()
        .setDatanode(datanode)
        .build();
  }

  public SCMCloseContainerResponseProto closeContainer(
      SCMCloseContainerRequestProto request)
      throws IOException {
    try {
      impl.closeContainer(request.getContainerID());
    } catch (SCMException exception) {
      if (exception.getResult()
          .equals(SCMException.ResultCodes.CONTAINER_ALREADY_CLOSED)) {
        return SCMCloseContainerResponseProto.newBuilder()
            .setStatus(CONTAINER_ALREADY_CLOSED).build();
      }
      if (exception.getResult()
          .equals(SCMException.ResultCodes.CONTAINER_ALREADY_CLOSING)) {
        return SCMCloseContainerResponseProto.newBuilder()
            .setStatus(CONTAINER_ALREADY_CLOSING).build();
      }
      throw exception;
    }
    return SCMCloseContainerResponseProto.newBuilder().build();
  }

  public PipelineResponseProto allocatePipeline(
      StorageContainerLocationProtocolProtos.PipelineRequestProto request,
      int clientVersion) throws IOException {
    Pipeline pipeline = impl.createReplicationPipeline(
        request.getReplicationType(), request.getReplicationFactor(),
        HddsProtos.NodePool.getDefaultInstance());
    if (pipeline == null) {
      return PipelineResponseProto.newBuilder()
          .setErrorCode(errorPipelineAlreadyExists).build();
    }
    return PipelineResponseProto.newBuilder()
        .setErrorCode(success)
        .setPipeline(pipeline.getProtobufMessage(clientVersion)).build();
  }

  public ListPipelineResponseProto listPipelines(
      ListPipelineRequestProto request, int clientVersion)
      throws IOException {
    ListPipelineResponseProto.Builder builder = ListPipelineResponseProto
        .newBuilder();
    List<Pipeline> pipelines = impl.listPipelines();
    for (Pipeline pipeline : pipelines) {
      builder.addPipelines(pipeline.getProtobufMessage(clientVersion));
    }
    return builder.build();
  }

  public GetPipelineResponseProto getPipeline(
      GetPipelineRequestProto request,
      int clientVersion) throws IOException {
    GetPipelineResponseProto.Builder builder = GetPipelineResponseProto
        .newBuilder();
    Pipeline pipeline = impl.getPipeline(request.getPipelineID());
    builder.setPipeline(pipeline.getProtobufMessage(clientVersion));
    return builder.build();
  }

  public ActivatePipelineResponseProto activatePipeline(
      ActivatePipelineRequestProto request)
      throws IOException {
    impl.activatePipeline(request.getPipelineID());
    return ActivatePipelineResponseProto.newBuilder().build();
  }

  public DeactivatePipelineResponseProto deactivatePipeline(
      DeactivatePipelineRequestProto request)
      throws IOException {
    impl.deactivatePipeline(request.getPipelineID());
    return DeactivatePipelineResponseProto.newBuilder().build();
  }

  public ClosePipelineResponseProto closePipeline(
      ClosePipelineRequestProto request)
      throws IOException {

    impl.closePipeline(request.getPipelineID());
    return ClosePipelineResponseProto.newBuilder().build();

  }

  public HddsProtos.GetScmInfoResponseProto getScmInfo(
      HddsProtos.GetScmInfoRequestProto req)
      throws IOException {
    ScmInfo scmInfo = impl.getScmInfo();
    return HddsProtos.GetScmInfoResponseProto.newBuilder()
        .setClusterId(scmInfo.getClusterId())
        .setScmId(scmInfo.getScmId())
        .addAllPeerRoles(scmInfo.getPeerRoles())
        .build();
  }

  public InSafeModeResponseProto inSafeMode(
      InSafeModeRequestProto request) throws IOException {

    return InSafeModeResponseProto.newBuilder()
        .setInSafeMode(impl.inSafeMode()).build();

  }

  public GetSafeModeRuleStatusesResponseProto getSafeModeRuleStatues(
      GetSafeModeRuleStatusesRequestProto request) throws IOException {
    Map<String, Pair<Boolean, String>>
        map = impl.getSafeModeRuleStatuses();
    List<SafeModeRuleStatusProto> proto = new ArrayList();
    for (Map.Entry<String, Pair<Boolean, String>> entry : map.entrySet()) {
      proto.add(SafeModeRuleStatusProto.newBuilder().setRuleName(entry.getKey())
          .setValidate(entry.getValue().getLeft())
          .setStatusText(entry.getValue().getRight())
          .build());
    }
    return GetSafeModeRuleStatusesResponseProto.newBuilder()
        .addAllSafeModeRuleStatusesProto(proto).build();
  }

  public FinalizeScmUpgradeResponseProto getFinalizeScmUpgrade(
      FinalizeScmUpgradeRequestProto request) throws IOException {
    StatusAndMessages progress =
        impl.finalizeScmUpgrade(request.getUpgradeClientId());

    UpgradeFinalizationStatus.Status protoStatus =
        UpgradeFinalizationStatus.Status.valueOf(progress.status().name());

    UpgradeFinalizationStatus response =
        UpgradeFinalizationStatus.newBuilder()
            .setStatus(protoStatus)
            .addAllMessages(progress.msgs())
            .build();

    return FinalizeScmUpgradeResponseProto.newBuilder()
        .setStatus(response)
        .build();
  }

  public QueryUpgradeFinalizationProgressResponseProto
      getQueryUpgradeFinalizationProgress(
          QueryUpgradeFinalizationProgressRequestProto request)
      throws IOException {
    StatusAndMessages progress =
        impl.queryUpgradeFinalizationProgress(request.getUpgradeClientId(),
            request.getTakeover(), request.getReadonly());

    UpgradeFinalizationStatus.Status protoStatus =
        UpgradeFinalizationStatus.Status.valueOf(progress.status().name());

    UpgradeFinalizationStatus response =
        UpgradeFinalizationStatus.newBuilder()
            .setStatus(protoStatus)
            .addAllMessages(progress.msgs())
            .build();

    return QueryUpgradeFinalizationProgressResponseProto.newBuilder()
        .setStatus(response)
        .build();
  }

  public ForceExitSafeModeResponseProto forceExitSafeMode(
      ForceExitSafeModeRequestProto request)
      throws IOException {
    return ForceExitSafeModeResponseProto.newBuilder()
        .setExitedSafeMode(impl.forceExitSafeMode()).build();

  }

  public StartReplicationManagerResponseProto startReplicationManager(
      StartReplicationManagerRequestProto request)
      throws IOException {
    impl.startReplicationManager();
    return StartReplicationManagerResponseProto.newBuilder().build();
  }

  public StopReplicationManagerResponseProto stopReplicationManager(
      StopReplicationManagerRequestProto request)
      throws IOException {
    impl.stopReplicationManager();
    return StopReplicationManagerResponseProto.newBuilder().build();

  }

  public ReplicationManagerStatusResponseProto getReplicationManagerStatus(
      ReplicationManagerStatusRequestProto request)
      throws IOException {
    return ReplicationManagerStatusResponseProto.newBuilder()
        .setIsRunning(impl.getReplicationManagerStatus()).build();
  }

  public ReplicationManagerReportResponseProto getReplicationManagerReport(
      ReplicationManagerReportRequestProto request) throws IOException {
    return ReplicationManagerReportResponseProto.newBuilder()
        .setReport(impl.getReplicationManagerReport().toProtobuf())
        .build();
  }

  public StartContainerBalancerResponseProto startContainerBalancer(
      StartContainerBalancerRequestProto request)
      throws IOException {
    Optional<Double> threshold = Optional.empty();
    Optional<Integer> iterations = Optional.empty();
    Optional<Integer> maxDatanodesPercentageToInvolvePerIteration =
        Optional.empty();
    Optional<Long> maxSizeToMovePerIterationInGB = Optional.empty();
    Optional<Long> maxSizeEnteringTargetInGB = Optional.empty();
    Optional<Long> maxSizeLeavingSourceInGB = Optional.empty();
    Optional<Integer> balancingInterval = Optional.empty();
    Optional<Integer> moveTimeout = Optional.empty();
    Optional<Integer> moveReplicationTimeout = Optional.empty();
    Optional<Boolean> networkTopologyEnable = Optional.empty();
    Optional<String> includeNodes = Optional.empty();
    Optional<String> excludeNodes = Optional.empty();
    Optional<String> excludeContainers = Optional.empty();

    if (request.hasThreshold()) {
      threshold = Optional.of(request.getThreshold());
    }

    if (request.hasIterations()) {
      iterations = Optional.of(request.getIterations());
    } else if (request.hasIdleiterations()) {
      iterations = Optional.of(request.getIdleiterations());
    }

    if (request.hasMaxDatanodesPercentageToInvolvePerIteration()) {
      maxDatanodesPercentageToInvolvePerIteration =
          Optional.of(request.getMaxDatanodesPercentageToInvolvePerIteration());
    } else if (request.hasMaxDatanodesRatioToInvolvePerIteration()) {
      maxDatanodesPercentageToInvolvePerIteration =
          Optional.of(
              (int) (request.getMaxDatanodesRatioToInvolvePerIteration() *
                  100));
    }

    if (request.hasMaxSizeToMovePerIterationInGB()) {
      maxSizeToMovePerIterationInGB =
          Optional.of(request.getMaxSizeToMovePerIterationInGB());
    }

    if (request.hasMaxSizeEnteringTargetInGB()) {
      maxSizeEnteringTargetInGB =
          Optional.of(request.getMaxSizeEnteringTargetInGB());
    }

    if (request.hasMaxSizeLeavingSourceInGB()) {
      maxSizeLeavingSourceInGB =
          Optional.of(request.getMaxSizeLeavingSourceInGB());
    }

    if (request.hasBalancingInterval()) {
      balancingInterval = Optional.of(request.getBalancingInterval());
    }

    if (request.hasMoveTimeout()) {
      moveTimeout = Optional.of(request.getMoveTimeout());
    }

    if (request.hasMoveReplicationTimeout()) {
      moveReplicationTimeout = Optional.of(request.getMoveReplicationTimeout());
    }

    if (request.hasNetworkTopologyEnable()) {
      networkTopologyEnable = Optional.of(request.getNetworkTopologyEnable());
    }

    if (request.hasIncludeNodes()) {
      includeNodes = Optional.of(request.getIncludeNodes());
    }

    if (request.hasExcludeNodes()) {
      excludeNodes = Optional.of(request.getExcludeNodes());
    }

    if (request.hasExcludeContainers()) {
      excludeContainers = Optional.of(request.getExcludeContainers());
    }

    return impl.startContainerBalancer(threshold, iterations,
        maxDatanodesPercentageToInvolvePerIteration,
        maxSizeToMovePerIterationInGB, maxSizeEnteringTargetInGB,
        maxSizeLeavingSourceInGB, balancingInterval, moveTimeout,
        moveReplicationTimeout, networkTopologyEnable, includeNodes,
        excludeNodes, excludeContainers);
  }

  public StopContainerBalancerResponseProto stopContainerBalancer(
      StopContainerBalancerRequestProto request)
      throws IOException {
    impl.stopContainerBalancer();
    return StopContainerBalancerResponseProto.newBuilder().build();

  }

  public ContainerBalancerStatusResponseProto getContainerBalancerStatus(
      ContainerBalancerStatusRequestProto request)
      throws IOException {
    return ContainerBalancerStatusResponseProto.newBuilder()
        .setIsRunning(impl.getContainerBalancerStatus()).build();
  }

  public ContainerBalancerStatusInfoResponseProto getContainerBalancerStatusInfo(
          StorageContainerLocationProtocolProtos.ContainerBalancerStatusInfoRequestProto request)
          throws IOException {
    return impl.getContainerBalancerStatusInfo();
  }

  public DecommissionNodesResponseProto decommissionNodes(
      DecommissionNodesRequestProto request) throws IOException {
    List<DatanodeAdminError> errors =
        impl.decommissionNodes(request.getHostsList(), request.getForce());
    DecommissionNodesResponseProto.Builder response =
        DecommissionNodesResponseProto.newBuilder();
    for (DatanodeAdminError e : errors) {
      DatanodeAdminErrorResponseProto.Builder error =
          DatanodeAdminErrorResponseProto.newBuilder();
      error.setHost(e.getHostname());
      error.setError(e.getError());
      response.addFailedHosts(error);
    }
    return response.build();
  }

  public GetContainersOnDecomNodeResponseProto getContainersOnDecomNode(GetContainersOnDecomNodeRequestProto request)
      throws IOException {
    Map<String, List<ContainerID>> containerMap = impl.getContainersOnDecomNode(
        DatanodeDetails.getFromProtoBuf(request.getDatanodeDetails()));
    List<ContainersOnDecomNodeProto> containersProtoList = new ArrayList<>();
    for (Map.Entry<String, List<ContainerID>> containerList : containerMap.entrySet()) {
      List<HddsProtos.ContainerID> containerIdsProto = new ArrayList<>();
      for (ContainerID id : containerList.getValue()) {
        containerIdsProto.add(id.getProtobuf());
      }
      containersProtoList.add(ContainersOnDecomNodeProto.newBuilder().setName(containerList.getKey())
          .addAllId(containerIdsProto).build());
    }
    return GetContainersOnDecomNodeResponseProto.newBuilder().addAllContainersOnDecomNode(containersProtoList).build();
  }

  public RecommissionNodesResponseProto recommissionNodes(
      RecommissionNodesRequestProto request) throws IOException {
    List<DatanodeAdminError> errors =
        impl.recommissionNodes(request.getHostsList());
    RecommissionNodesResponseProto.Builder response =
        RecommissionNodesResponseProto.newBuilder();
    for (DatanodeAdminError e : errors) {
      DatanodeAdminErrorResponseProto.Builder error =
          DatanodeAdminErrorResponseProto.newBuilder();
      error.setHost(e.getHostname());
      error.setError(e.getError());
      response.addFailedHosts(error);
    }
    return response.build();
  }

  public StartMaintenanceNodesResponseProto startMaintenanceNodes(
      StartMaintenanceNodesRequestProto request) throws IOException {
    List<DatanodeAdminError> errors =
        impl.startMaintenanceNodes(request.getHostsList(),
        (int)request.getEndInHours(), request.getForce());
    StartMaintenanceNodesResponseProto.Builder response =
        StartMaintenanceNodesResponseProto.newBuilder();
    for (DatanodeAdminError e : errors) {
      DatanodeAdminErrorResponseProto.Builder error =
          DatanodeAdminErrorResponseProto.newBuilder();
      error.setHost(e.getHostname());
      error.setError(e.getError());
      response.addFailedHosts(error);
    }
    return response.build();
  }

  public DatanodeUsageInfoResponseProto getDatanodeUsageInfo(
      StorageContainerLocationProtocolProtos.DatanodeUsageInfoRequestProto
          request, int clientVersion) throws IOException {
    List<HddsProtos.DatanodeUsageInfoProto> infoList;

    // get info by ip or uuid
    if (request.hasUuid() || request.hasIpaddress()) {
      infoList = impl.getDatanodeUsageInfo(request.getIpaddress(),
          request.getUuid(), clientVersion);
    } else {  // get most or least used nodes
      infoList = impl.getDatanodeUsageInfo(request.getMostUsed(),
          request.getCount(), clientVersion);
    }

    return DatanodeUsageInfoResponseProto.newBuilder()
        .addAllInfo(infoList)
        .build();
  }

  public GetContainerCountResponseProto getContainerCount(
      StorageContainerLocationProtocolProtos.GetContainerCountRequestProto
      request) throws IOException {

    return GetContainerCountResponseProto.newBuilder()
      .setContainerCount(impl.getContainerCount())
      .build();
  }

  public GetContainerCountResponseProto getClosedContainerCount(
      StorageContainerLocationProtocolProtos.GetContainerCountRequestProto
          request) throws IOException {

    return GetContainerCountResponseProto.newBuilder()
        .setContainerCount(impl.getContainerCount(
            HddsProtos.LifeCycleState.CLOSED))
        .build();
  }

  @Deprecated
  public GetFailedDeletedBlocksTxnResponseProto getFailedDeletedBlocksTxn(
      GetFailedDeletedBlocksTxnRequestProto request) throws IOException {
    long startTxId = request.hasStartTxId() ? request.getStartTxId() : 0;
    return GetFailedDeletedBlocksTxnResponseProto.newBuilder()
        .addAllDeletedBlocksTransactions(
            impl.getFailedDeletedBlockTxn(request.getCount(), startTxId))
        .build();
  }

  @Deprecated
  public ResetDeletedBlockRetryCountResponseProto
      getResetDeletedBlockRetryCount(ResetDeletedBlockRetryCountRequestProto
      request) throws IOException {
    return ResetDeletedBlockRetryCountResponseProto.newBuilder()
        .setResetCount(impl.resetDeletedBlockRetryCount(
            request.getTransactionIdList()))
        .build();
  }

  public GetDeletedBlocksTxnSummaryResponseProto getDeletedBlocksTxnSummary(
      GetDeletedBlocksTxnSummaryRequestProto request) throws IOException {
    HddsProtos.DeletedBlocksTransactionSummary summary = impl.getDeletedBlockSummary();
    if (summary == null) {
      return GetDeletedBlocksTxnSummaryResponseProto.newBuilder().build();
    } else {
      return GetDeletedBlocksTxnSummaryResponseProto.newBuilder()
          .setSummary(summary)
          .build();
    }
  }

  public TransferLeadershipResponseProto transferScmLeadership(
      TransferLeadershipRequestProto request) throws IOException {
    String newLeaderId = request.getNewLeaderId();
    impl.transferLeadership(newLeaderId);
    return TransferLeadershipResponseProto.getDefaultInstance();
  }

  public DecommissionScmResponseProto decommissionScm(
      DecommissionScmRequestProto request) throws IOException {
    return impl.decommissionScm(
            request.getScmId());
  }

  public GetMetricsResponseProto getMetrics(GetMetricsRequestProto request) throws IOException {
    return GetMetricsResponseProto.newBuilder().setMetricsJson(impl.getMetrics(request.getQuery())).build();
  }

  public ReconcileContainerResponseProto reconcileContainer(ReconcileContainerRequestProto request) throws IOException {
    impl.reconcileContainer(request.getContainerID());
    return ReconcileContainerResponseProto.getDefaultInstance();
  }

}
