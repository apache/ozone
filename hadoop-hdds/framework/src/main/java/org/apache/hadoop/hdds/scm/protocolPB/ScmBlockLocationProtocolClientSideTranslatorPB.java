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

import static org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.Status.OK;

import com.google.common.base.Preconditions;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.AllocateScmBlockResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteScmKeyBlocksRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.DeleteScmKeyBlocksResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.GetClusterTreeRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.GetClusterTreeResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.KeyBlocks;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SCMBlockLocationRequest;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SCMBlockLocationResponse;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SortDatanodesRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.SortDatanodesResponseProto;
import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos.Type;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.net.InnerNodeImpl;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.proxy.SCMBlockLocationFailoverProxyProvider;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc_.ProtobufHelper;
import org.apache.hadoop.ipc_.ProtocolTranslator;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is the client-side translator to translate the requests made on
 * the {@link ScmBlockLocationProtocol} interface to the RPC server
 * implementing {@link ScmBlockLocationProtocolPB}.
 */
@InterfaceAudience.Private
public final class ScmBlockLocationProtocolClientSideTranslatorPB
    implements ScmBlockLocationProtocol, ProtocolTranslator, Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(ScmBlockLocationProtocolClientSideTranslatorPB.class);

  private static final double RATIS_LIMIT_FACTOR = 0.9;
  private int ratisByteLimit;

  /**
   * RpcController is not used and hence is set to null.
   */
  private static final RpcController NULL_RPC_CONTROLLER = null;

  private final ScmBlockLocationProtocolPB rpcProxy;
  private SCMBlockLocationFailoverProxyProvider failoverProxyProvider;

  /**
   * Creates a new StorageContainerLocationProtocolClientSideTranslatorPB.
   *
   * @param proxyProvider {@link SCMBlockLocationFailoverProxyProvider}
   * failover proxy provider.
   */
  public ScmBlockLocationProtocolClientSideTranslatorPB(
      SCMBlockLocationFailoverProxyProvider proxyProvider, OzoneConfiguration conf) {
    Preconditions.checkState(proxyProvider != null);
    this.failoverProxyProvider = proxyProvider;
    this.rpcProxy = (ScmBlockLocationProtocolPB) RetryProxy.create(
        ScmBlockLocationProtocolPB.class, failoverProxyProvider,
        failoverProxyProvider.getRetryPolicy());
    int limit = (int) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        ScmConfigKeys.OZONE_SCM_HA_RAFT_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT,
        StorageUnit.BYTES);
    // always go to 90% of max limit for request as other header will be added
    this.ratisByteLimit = (int) (limit * RATIS_LIMIT_FACTOR);
  }

  /**
   * Returns a SCMBlockLocationRequest builder with specified type.
   * @param cmdType type of the request
   */
  private SCMBlockLocationRequest.Builder createSCMBlockRequest(Type cmdType) {
    return SCMBlockLocationRequest.newBuilder()
        .setCmdType(cmdType)
        .setVersion(ClientVersion.CURRENT.serialize())
        .setTraceID(TracingUtil.exportCurrentSpan());
  }

  /**
   * Submits client request to SCM server.
   * @param req client request
   * @return response from SCM
   * @throws IOException thrown if any Protobuf service exception occurs
   */
  private SCMBlockLocationResponse submitRequest(
      SCMBlockLocationRequest req) throws IOException {
    try {
      SCMBlockLocationResponse response =
          rpcProxy.send(NULL_RPC_CONTROLLER, req);
      return response;
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  private SCMBlockLocationResponse handleError(SCMBlockLocationResponse resp)
      throws SCMException {
    if (resp.getStatus() != OK) {
      throw new SCMException(resp.getMessage(),
          SCMException.ResultCodes.values()[resp.getStatus().ordinal()]);
    }
    return resp;
  }

  /**
   * Asks SCM where a block should be allocated. SCM responds with the
   * set of datanodes that should be used creating this block.
   *
   * @param size              - size of the block.
   * @param num               - number of blocks.
   * @param replicationConfig - replication configuration of the blocks.
   * @param excludeList       - exclude list while allocating blocks.
   * @param clientMachine     - client address, depends, can be hostname or
   *                            ipaddress.
   * @return allocated block accessing info (key, pipeline).
   * @throws IOException
   */
  @Override
  public List<AllocatedBlock> allocateBlock(
      long size, int num,
      ReplicationConfig replicationConfig,
      String owner, ExcludeList excludeList,
      String clientMachine
  ) throws IOException {
    Preconditions.checkArgument(size > 0, "block size must be greater than 0");

    final AllocateScmBlockRequestProto.Builder requestBuilder =
        AllocateScmBlockRequestProto.newBuilder()
            .setSize(size)
            .setNumBlocks(num)
            .setType(replicationConfig.getReplicationType())
            .setOwner(owner)
            .setExcludeList(excludeList.getProtoBuf());

    if (StringUtils.isNotEmpty(clientMachine)) {
      requestBuilder.setClient(clientMachine);
    }

    switch (replicationConfig.getReplicationType()) {
    case STAND_ALONE:
      requestBuilder.setFactor(
          ((StandaloneReplicationConfig) replicationConfig)
              .getReplicationFactor());
      break;
    case RATIS:
      requestBuilder.setFactor(
          ((RatisReplicationConfig) replicationConfig).getReplicationFactor());
      break;
    case EC:
      // We do not check for server support here, as this call is used only
      // from OM which has the same software version as SCM.
      // TODO: Rolling upgrade support needs to change this.
      requestBuilder.setEcReplicationConfig(
          ((ECReplicationConfig)replicationConfig).toProto());
      break;
    default:
      throw new IllegalArgumentException(
          "Unsupported replication type " + replicationConfig
              .getReplicationType());
    }

    AllocateScmBlockRequestProto request = requestBuilder.build();

    SCMBlockLocationRequest wrapper = createSCMBlockRequest(
        Type.AllocateScmBlock)
        .setAllocateScmBlockRequest(request)
        .build();

    final SCMBlockLocationResponse wrappedResponse =
        handleError(submitRequest(wrapper));
    final AllocateScmBlockResponseProto response =
        wrappedResponse.getAllocateScmBlockResponse();

    List<AllocatedBlock> blocks = new ArrayList<>(response.getBlocksCount());
    for (AllocateBlockResponse resp : response.getBlocksList()) {
      AllocatedBlock.Builder builder = new AllocatedBlock.Builder()
          .setContainerBlockID(
              ContainerBlockID.getFromProtobuf(resp.getContainerBlockID()))
          .setPipeline(Pipeline.getFromProtobuf(resp.getPipeline()));
      blocks.add(builder.build());
    }

    return blocks;
  }

  /**
   * Delete the set of keys specified.
   *
   * @param keyBlocksInfoList batch of block keys to delete.
   * @return list of block deletion results.
   * @throws IOException if there is any failure.
   *
   */
  @Override
  public List<DeleteBlockGroupResult> deleteKeyBlocks(
      List<BlockGroup> keyBlocksInfoList) throws IOException {

    List<DeleteBlockGroupResult> allResults = new ArrayList<>();
    List<KeyBlocks> batch = new ArrayList<>();

    int serializedSize = 0;
    for (BlockGroup bg : keyBlocksInfoList) {
      KeyBlocks bgProto = bg.getProto();
      int currSize = bgProto.getSerializedSize();
      if (currSize + serializedSize > ratisByteLimit) {
        allResults.addAll(submitDeleteKeyBlocks(batch));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Sending batch of {} KeyBlocks (~{} bytes)", batch.size(), serializedSize);
        }
        serializedSize = 0;
        batch.clear();
      }
      batch.add(bgProto);
      serializedSize += currSize;
    }

    if (!batch.isEmpty()) {
      allResults.addAll(submitDeleteKeyBlocks(batch));
    }

    return allResults;
  }

  private List<DeleteBlockGroupResult> submitDeleteKeyBlocks(List<KeyBlocks> batch)
      throws IOException {
    DeleteScmKeyBlocksRequestProto request = DeleteScmKeyBlocksRequestProto
        .newBuilder()
        .addAllKeyBlocks(batch)
        .build();
    SCMBlockLocationRequest wrapper = createSCMBlockRequest(
        Type.DeleteScmKeyBlocks)
        .setDeleteScmKeyBlocksRequest(request)
        .build();
    final SCMBlockLocationResponse wrappedResponse =
        handleError(submitRequest(wrapper));
    final DeleteScmKeyBlocksResponseProto resp =
        wrappedResponse.getDeleteScmKeyBlocksResponse();

    List<DeleteBlockGroupResult> results =
        new ArrayList<>(resp.getResultsCount());
    results.addAll(resp.getResultsList().stream().map(
        result -> new DeleteBlockGroupResult(result.getObjectKey(),
            DeleteBlockGroupResult
                .convertBlockResultProto(result.getBlockResultsList())))
        .collect(Collectors.toList()));
    return results;
  }

  /**
   * Gets the cluster Id and Scm Id from SCM.
   * @return ScmInfo
   * @throws IOException
   */
  @Override
  public ScmInfo getScmInfo() throws IOException {
    HddsProtos.GetScmInfoRequestProto request =
        HddsProtos.GetScmInfoRequestProto.getDefaultInstance();
    HddsProtos.GetScmInfoResponseProto resp;

    SCMBlockLocationRequest wrapper = createSCMBlockRequest(
        Type.GetScmInfo)
        .setGetScmInfoRequest(request)
        .build();

    final SCMBlockLocationResponse wrappedResponse =
        handleError(submitRequest(wrapper));
    resp = wrappedResponse.getGetScmInfoResponse();
    ScmInfo.Builder builder = new ScmInfo.Builder()
        .setClusterId(resp.getClusterId())
        .setScmId(resp.getScmId());
    return builder.build();
  }

  /**
   * Request to add SCM to existing SCM HA group.
   * @return status
   * @throws IOException
   */
  @Override
  public boolean addSCM(AddSCMRequest request) throws IOException {
    HddsProtos.AddScmRequestProto requestProto =
        request.getProtobuf();
    HddsProtos.AddScmResponseProto resp;
    SCMBlockLocationRequest wrapper = createSCMBlockRequest(
        Type.AddScm)
        .setAddScmRequestProto(requestProto)
        .build();

    final SCMBlockLocationResponse wrappedResponse =
        handleError(submitRequest(wrapper));
    resp = wrappedResponse.getAddScmResponse();
    return resp.getSuccess();
  }

  /**
   * Sort the datanodes based on distance from client.
   * @return list of datanodes;
   * @throws IOException
   */
  @Override
  public List<DatanodeDetails> sortDatanodes(List<String> nodes,
      String clientMachine) throws IOException {
    SortDatanodesRequestProto request = SortDatanodesRequestProto
        .newBuilder()
        .addAllNodeNetworkName(nodes)
        .setClient(clientMachine)
        .build();
    SCMBlockLocationRequest wrapper = createSCMBlockRequest(
        Type.SortDatanodes)
        .setSortDatanodesRequest(request)
        .build();

    final SCMBlockLocationResponse wrappedResponse =
        handleError(submitRequest(wrapper));
    SortDatanodesResponseProto resp =
        wrappedResponse.getSortDatanodesResponse();
    List<DatanodeDetails> results = new ArrayList<>(resp.getNodeCount());
    results.addAll(resp.getNodeList().stream()
        .map(node -> DatanodeDetails.getFromProtoBuf(node))
        .collect(Collectors.toList()));
    return results;
  }

  @Override
  public InnerNode getNetworkTopology() throws IOException {
    GetClusterTreeRequestProto request =
        GetClusterTreeRequestProto.newBuilder().build();
    SCMBlockLocationRequest wrapper = createSCMBlockRequest(Type.GetClusterTree)
        .setGetClusterTreeRequest(request)
        .build();

    final SCMBlockLocationResponse wrappedResponse =
        handleError(submitRequest(wrapper));
    GetClusterTreeResponseProto resp =
        wrappedResponse.getGetClusterTreeResponse();

    return (InnerNode) setParent(
        InnerNodeImpl.fromProtobuf(resp.getClusterTree()));
  }

  /**
   * Sets the parent field for the clusterTree nodes recursively.
   *
   * @param node cluster tree without parents set.
   * @return updated cluster tree with parents set.
   */
  private Node setParent(Node node) {
    if (node instanceof InnerNodeImpl) {
      InnerNodeImpl innerNode = (InnerNodeImpl) node;
      if (innerNode.getChildrenMap() != null) {
        for (Map.Entry<String, Node> child : innerNode.getChildrenMap()
            .entrySet()) {
          child.getValue().setParent(innerNode);
          setParent(child.getValue());
        }
      }
    }
    return node;
  }

  @Override
  public Object getUnderlyingProxyObject() {
    return rpcProxy;
  }

  @Override
  public void close() throws IOException {
    failoverProxyProvider.close();
  }
}
