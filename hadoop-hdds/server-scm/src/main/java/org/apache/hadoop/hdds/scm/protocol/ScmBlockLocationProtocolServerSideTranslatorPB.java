/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.protocol;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteKeyBlocksResultProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteScmKeyBlocksRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteScmKeyBlocksResponseProto;
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
import org.apache.hadoop.hdds.scm.ha.RetriableWithNoFailoverException;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolPB;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.OzoneProtocolMessageDispatcher;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;

import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
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

  private static final Logger LOG = LoggerFactory
      .getLogger(ScmBlockLocationProtocolServerSideTranslatorPB.class);

  private final OzoneProtocolMessageDispatcher<SCMBlockLocationRequest,
      SCMBlockLocationResponse, ProtocolMessageEnum>
      dispatcher;

  /**
   * Creates a new ScmBlockLocationProtocolServerSideTranslatorPB.
   *
   * @param impl {@link ScmBlockLocationProtocol} server implementation
   */
  public ScmBlockLocationProtocolServerSideTranslatorPB(
      ScmBlockLocationProtocol impl,
      StorageContainerManager scm,
      ProtocolMessageMetrics<ProtocolMessageEnum> metrics)
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
    if (!scm.getScmContext().isLeader()) {
      throw new ServiceException(scm.getScmHAManager()
                                    .getRatisServer()
                                    .triggerNotLeaderException());
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
      default:
        // Should never happen
        throw new IOException("Unknown Operation " + request.getCmdType() +
            " in ScmBlockLocationProtocol");
      }
    } catch (IOException e) {
      if (SCMHAUtils.isRetriableWithNoFailoverException(e)) {
        throw new ServiceException(new RetriableWithNoFailoverException(e));
      } else if (e instanceof NotLeaderException) {
        throw new ServiceException(e);
      }
      response.setSuccess(false);
      response.setStatus(exceptionToResponseStatus(e));
      if (e.getMessage() != null) {
        response.setMessage(e.getMessage());
      }
    }

    return response.build();
  }

  private Status exceptionToResponseStatus(IOException ex) {
    if (ex instanceof SCMException) {
      return Status.values()[((SCMException) ex).getResult().ordinal()];
    } else {
      return Status.INTERNAL_ERROR;
    }
  }

  public AllocateScmBlockResponseProto allocateScmBlock(
      AllocateScmBlockRequestProto request, int clientVersion)
      throws IOException {
    List<AllocatedBlock> allocatedBlocks =
        impl.allocateBlock(request.getSize(),
            request.getNumBlocks(),
            ReplicationConfig.fromProto(
                request.getType(),
                request.getFactor()),
            request.getOwner(),
            ExcludeList.getFromProtoBuf(request.getExcludeList()));

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
          .setPipeline(block.getPipeline().getProtobufMessage(clientVersion)));
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
}
