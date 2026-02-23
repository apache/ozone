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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteKeyBlocksResultProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteScmKeyBlocksRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteScmKeyBlocksResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.GetClusterTreeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SCMBlockLocationRequest;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SCMBlockLocationResponse;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SortDatanodesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SortDatanodesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.Status;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.RatisUtil;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the server-side translator that forwards requests received on
 * {@link StorageContainerLocationProtocolPB} to the
 * {@link StorageContainerLocationProtocol} server implementation.
 */
@InterfaceAudience.Private
public final class ScmBlockLocationProtocolServerSideTranslatorPB
    implements ScmBlockLocationProtocolPB {

  private final ScmBlockLocationProtocol impl;
  private final StorageContainerManager scm;
  private static final String ROLE_TYPE = "SCM";

  private static final Logger LOG = LoggerFactory
      .getLogger(ScmBlockLocationProtocolServerSideTranslatorPB.class);

  private final OzoneProtocolMessageDispatcher<SCMBlockLocationRequest,
      SCMBlockLocationResponse, ScmBlockLocationProtocolProtos.Type>
      dispatcher;

  /**
   * Creates a new ScmBlockLocationProtocolServerSideTranslatorPB.
   *
   * @param impl {@link ScmBlockLocationProtocol} server implementation
   */
  public ScmBlockLocationProtocolServerSideTranslatorPB(
      ScmBlockLocationProtocol impl,
      StorageContainerManager scm,
      ProtocolMessageMetrics<ScmBlockLocationProtocolProtos.Type> metrics)
      throws IOException {
    this.impl = impl;
    this.scm = scm;
    dispatcher = new OzoneProtocolMessageDispatcher<>(
        "BlockLocationProtocol", metrics, LOG);

  }

  private SCMBlockLocationResponse.Builder createSCMBlockResponse(
      ScmBlockLocationProtocolProtos.Type cmdType,
      String traceID) {
    return SCMBlockLocationResponse.newBuilder()
        .setCmdType(cmdType)
        .setTraceID(traceID);
  }

  @Override
  public SCMBlockLocationResponse send(RpcController controller,
      SCMBlockLocationRequest request) throws ServiceException {
    if (!scm.checkLeader()) {
      RatisUtil.checkRatisException(
          scm.getScmHAManager().getRatisServer().triggerNotLeaderException(),
          scm.getBlockProtocolRpcPort(), scm.getScmId(), scm.getHostname(), ROLE_TYPE);
    }
    return dispatcher.processRequest(
        request,
        this::processMessage,
        request.getCmdType(),
        request.getTraceID());
  }

  private SCMBlockLocationResponse processMessage(
      SCMBlockLocationRequest request) throws ServiceException {
    SCMBlockLocationResponse.Builder response = createSCMBlockResponse(
        request.getCmdType(),
        request.getTraceID());
    response.setSuccess(true);
    response.setStatus(Status.OK);

    try {
      switch (request.getCmdType()) {
      case AllocateScmBlock:
        if (scm.getLayoutVersionManager().needsFinalization() &&
            !scm.getLayoutVersionManager()
                .isAllowed(HDDSLayoutFeature.ERASURE_CODED_STORAGE_SUPPORT)
        ) {
          if (request.getAllocateScmBlockRequest().hasEcReplicationConfig()) {
            throw new SCMException("Cluster is not finalized yet, it is"
                + " not enabled to create blocks with Erasure Coded"
                + " replication type.",
                SCMException.ResultCodes.INTERNAL_ERROR);
          }
        }
        response.setAllocateScmBlockResponse(allocateScmBlock(
            request.getAllocateScmBlockRequest(), request.getVersion()));
        break;
      case DeleteScmKeyBlocks:
        response.setDeleteScmKeyBlocksResponse(
            deleteScmKeyBlocks(request.getDeleteScmKeyBlocksRequest()));
        break;
      case GetScmInfo:
        response.setGetScmInfoResponse(
            getScmInfo(request.getGetScmInfoRequest()));
        break;
      case AddScm:
        response.setAddScmResponse(
            getAddSCMResponse(request.getAddScmRequestProto()));
        break;
      case SortDatanodes:
        response.setSortDatanodesResponse(sortDatanodes(
            request.getSortDatanodesRequest(), request.getVersion()
        ));
        break;
      case GetClusterTree:
        response.setGetClusterTreeResponse(
            getClusterTree(request.getVersion()));
        break;
      default:
        // Should never happen
        throw new IOException("Unknown Operation " + request.getCmdType() +
            " in ScmBlockLocationProtocol");
      }
    } catch (IOException e) {
      RatisUtil.checkRatisException(e, scm.getBlockProtocolRpcPort(),
          scm.getScmId(), scm.getHostname(), ROLE_TYPE);
      response.setSuccess(false);
      response.setStatus(exceptionToResponseStatus(e));
      if (e.getMessage() != null) {
        response.setMessage(e.getMessage());
      }
    }

    return response.build();
  }

  private Status exceptionToResponseStatus(IOException ex) {
    if (ex instanceof SCMException && ((SCMException) ex).getResult() != null) {
      return Status.values()[((SCMException) ex).getResult().ordinal()];
    } else {
      return Status.INTERNAL_ERROR;
    }
  }

  public AllocateScmBlockResponseProto allocateScmBlock(
      AllocateScmBlockRequestProto request, int clientVersion)
      throws IOException {
    StorageType storageType = request.hasStorageType()
        ? StorageType.valueOf(request.getStorageType())
        : StorageType.DEFAULT;

    List<AllocatedBlock> allocatedBlocks =
        impl.allocateBlock(request.getSize(),
            request.getNumBlocks(),
            ReplicationConfig.fromProto(
                request.getType(),
                request.getFactor(),
                request.getEcReplicationConfig()),
            request.getOwner(),
            ExcludeList.getFromProtoBuf(request.getExcludeList()),
            request.getClient(),
            storageType);

    AllocateScmBlockResponseProto.Builder builder =
        AllocateScmBlockResponseProto.newBuilder();

    if (allocatedBlocks.size() < request.getNumBlocks()) {
      throw new SCMException("Allocated " + allocatedBlocks.size() +
          " blocks. Requested " + request.getNumBlocks() + " blocks",
          SCMException.ResultCodes.FAILED_TO_ALLOCATE_ENOUGH_BLOCKS);
    }
    for (AllocatedBlock block : allocatedBlocks) {
      builder.addBlocks(AllocateBlockResponse.newBuilder()
          .setContainerBlockID(block.getBlockID().getProtobuf())
          .setPipeline(block.getPipeline().getProtobufMessage(clientVersion, Name.IO_PORTS)));
    }

    return builder.build();
  }

  public DeleteScmKeyBlocksResponseProto deleteScmKeyBlocks(
      DeleteScmKeyBlocksRequestProto req
  )
      throws IOException {
    DeleteScmKeyBlocksResponseProto.Builder resp =
        DeleteScmKeyBlocksResponseProto.newBuilder();

    List<BlockGroup> infoList = req.getKeyBlocksList().stream()
        .map(BlockGroup::getFromProto).collect(Collectors.toList());
    final List<DeleteBlockGroupResult> results =
        impl.deleteKeyBlocks(infoList);
    for (DeleteBlockGroupResult result : results) {
      DeleteKeyBlocksResultProto.Builder deleteResult =
          DeleteKeyBlocksResultProto
              .newBuilder()
              .setObjectKey(result.getObjectKey())
              .addAllBlockResults(result.getBlockResultProtoList());
      resp.addResults(deleteResult.build());
    }
    return resp.build();
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

  public HddsProtos.AddScmResponseProto getAddSCMResponse(
      HddsProtos.AddScmRequestProto req)
      throws IOException {
    boolean status = impl.addSCM(AddSCMRequest.getFromProtobuf(req));
    return HddsProtos.AddScmResponseProto.newBuilder()
        .setSuccess(status)
        .build();
  }

  public SortDatanodesResponseProto sortDatanodes(
      SortDatanodesRequestProto request, int clientVersion)
      throws ServiceException {
    SortDatanodesResponseProto.Builder resp =
        SortDatanodesResponseProto.newBuilder();
    try {
      List<String> nodeList = request.getNodeNetworkNameList();
      final List<DatanodeDetails> results =
          impl.sortDatanodes(nodeList, request.getClient());
      if (results != null) {
        for (DatanodeDetails dn : results) {
          resp.addNode(dn.toProto(clientVersion));
        }
      }
      return resp.build();
    } catch (IOException ex) {
      throw new ServiceException(ex);
    }
  }

  public GetClusterTreeResponseProto getClusterTree(int clientVersion)
      throws IOException {
    GetClusterTreeResponseProto.Builder resp =
        GetClusterTreeResponseProto.newBuilder();
    InnerNode clusterTree = impl.getNetworkTopology();
    resp.setClusterTree(clusterTree.toProtobuf(clientVersion).getInnerNode());
    return resp.build();
  }
}
