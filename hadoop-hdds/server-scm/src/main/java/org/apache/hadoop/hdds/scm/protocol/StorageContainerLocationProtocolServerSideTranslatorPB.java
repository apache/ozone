/**
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
package org.apache.hadoop.hdds.scm.protocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ActivatePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ActivatePipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ClosePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ClosePipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DeactivatePipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DeactivatePipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ForceExitSafeModeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ForceExitSafeModeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineBatchRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineBatchResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetContainerWithPipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetPipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.InSafeModeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.InSafeModeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ListPipelineRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ListPipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.NodeQueryResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.PipelineResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReplicationManagerStatusRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ReplicationManagerStatusResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMCloseContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMCloseContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMDeleteContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMDeleteContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMListContainerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SCMListContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationRequest;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationResponse;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ScmContainerLocationResponse.Status;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartReplicationManagerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartReplicationManagerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StopReplicationManagerRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StopReplicationManagerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionNodesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionNodesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.RecommissionNodesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.RecommissionNodesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartMaintenanceNodesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.StartMaintenanceNodesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetSafeModeRuleStatusesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.GetSafeModeRuleStatusesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.SafeModeRuleStatusProto;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;

import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.PipelineResponseProto.Error.errorPipelineAlreadyExists;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.PipelineResponseProto.Error.success;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link StorageContainerLocationProtocolPB} to the
 * {@link StorageContainerLocationProtocol} server implementation.
 */
@InterfaceAudience.Private
public final class StorageContainerLocationProtocolServerSideTranslatorPB
    implements StorageContainerLocationProtocolPB {

  private static final Logger LOG =
      LoggerFactory.getLogger(
          StorageContainerLocationProtocolServerSideTranslatorPB.class);

  private final StorageContainerLocationProtocol impl;

  private OzoneProtocolMessageDispatcher<ScmContainerLocationRequest,
      ScmContainerLocationResponse, ProtocolMessageEnum>
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
      ProtocolMessageMetrics<ProtocolMessageEnum> protocolMetrics)
      throws IOException {
    this.impl = impl;
    this.dispatcher =
        new OzoneProtocolMessageDispatcher<>("ScmContainerLocation",
            protocolMetrics, LOG);
  }

  @Override
  public ScmContainerLocationResponse submitRequest(RpcController controller,
      ScmContainerLocationRequest request) throws ServiceException {
    return dispatcher
        .processRequest(request, this::processRequest, request.getCmdType(),
            request.getTraceID());
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
      case CloseContainer:
        return ScmContainerLocationResponse.newBuilder()
            .setCmdType(request.getCmdType())
            .setStatus(Status.OK)
            .setScmCloseContainerResponse(closeContainer(
                request.getScmCloseContainerRequest()))
            .build();
      case AllocatePipeline:
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
      default:
        throw new IllegalArgumentException(
            "Unknown command type: " + request.getCmdType());
      }
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  public ContainerResponseProto allocateContainer(ContainerRequestProto request,
      int clientVersion) throws IOException {
    ContainerWithPipeline cp = impl
        .allocateContainer(request.getReplicationType(),
            request.getReplicationFactor(), request.getOwner());
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
    List<ContainerInfo> containerList =
        impl.listContainer(startContainerID, count);
    SCMListContainerResponseProto.Builder builder =
        SCMListContainerResponseProto.newBuilder();
    for (ContainerInfo container : containerList) {
      builder.addContainers(container.getProtobuf());
    }
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

  public SCMCloseContainerResponseProto closeContainer(
      SCMCloseContainerRequestProto request)
      throws IOException {
    impl.closeContainer(request.getContainerID());
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

  public DecommissionNodesResponseProto decommissionNodes(
      DecommissionNodesRequestProto request) throws IOException {
    impl.decommissionNodes(request.getHostsList());
    return DecommissionNodesResponseProto.newBuilder()
        .build();
  }

  public RecommissionNodesResponseProto recommissionNodes(
      RecommissionNodesRequestProto request) throws IOException {
    impl.recommissionNodes(request.getHostsList());
    return RecommissionNodesResponseProto.newBuilder().build();
  }

  public StartMaintenanceNodesResponseProto startMaintenanceNodes(
      StartMaintenanceNodesRequestProto request) throws IOException {
    impl.startMaintenanceNodes(request.getHostsList(),
        (int)request.getEndInHours());
    return StartMaintenanceNodesResponseProto.newBuilder()
        .build();
  }

}
