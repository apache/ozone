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

package org.apache.hadoop.hdds.scm.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.BlockData;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.CloseContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.GetSmallFileResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ListBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ListBlockResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutSmallFileResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.common.helpers.BlockNotCommittedException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.security.token.Token;

import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of all container protocol calls performed by Container
 * clients.
 */
public final class ContainerProtocolCalls  {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerProtocolCalls.class);

  /**
   * There is no need to instantiate this class.
   */
  private ContainerProtocolCalls() {
  }

  /**
   * Calls the container protocol to list blocks in container.
   *
   * @param xceiverClient client to perform call
   * @param containerID the ID of the container to list block
   * @param startLocalID the localID of the first block to get
   * @param count max number of blocks to get
   * @param token a token for this block (may be null)
   * @return container protocol list block response
   * @throws IOException if there is an I/O error while performing the call
   */
  public static ListBlockResponseProto listBlock(XceiverClientSpi xceiverClient,
      long containerID, Long startLocalID, int count,
      Token<? extends TokenIdentifier> token) throws IOException {

    ListBlockRequestProto.Builder listBlockBuilder =
        ListBlockRequestProto.newBuilder()
            .setCount(count);

    if (startLocalID != null) {
      listBlockBuilder.setStartLocalID(startLocalID);
    }

    // datanodeID doesn't matter for read only requests
    String datanodeID =
        xceiverClient.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(Type.ListBlock)
            .setContainerID(containerID)
            .setDatanodeUuid(datanodeID)
            .setListBlock(listBlockBuilder.build());

    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }

    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto response =
        xceiverClient.sendCommand(request, getValidatorList());
    return response.getListBlock();
  }

  static <T> T tryEachDatanode(Pipeline pipeline,
      CheckedFunction<DatanodeDetails, T, IOException> op,
      Function<DatanodeDetails, String> toErrorMessage)
      throws IOException {
    final Set<DatanodeDetails> excluded = new HashSet<>();
    for (; ;) {
      final DatanodeDetails d = pipeline.getClosestNode(excluded);

      try {
        return op.apply(d);
      } catch (IOException e) {
        excluded.add(d);
        if (excluded.size() < pipeline.size()) {
          LOG.warn(toErrorMessage.apply(d)
              + "; will try another datanode.", e);
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * Calls the container protocol to get a container block.
   *
   * @param xceiverClient client to perform call
   * @param validators functions to validate the response
   * @param datanodeBlockID blockID to identify container
   * @param token a token for this block (may be null)
   * @return container protocol get block response
   * @throws IOException if there is an I/O error while performing the call
   */
  public static GetBlockResponseProto getBlock(XceiverClientSpi xceiverClient,
      List<CheckedBiFunction> validators,
      DatanodeBlockID datanodeBlockID,
      Token<? extends TokenIdentifier> token) throws IOException {
    GetBlockRequestProto.Builder readBlockRequest = GetBlockRequestProto
        .newBuilder()
        .setBlockID(datanodeBlockID);
    ContainerCommandRequestProto.Builder builder = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.GetBlock)
        .setContainerID(datanodeBlockID.getContainerID())
        .setGetBlock(readBlockRequest);
    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }

    return tryEachDatanode(xceiverClient.getPipeline(),
        d -> getBlock(xceiverClient, validators, builder, d),
        d -> toErrorMessage(datanodeBlockID, d));
  }

  static String toErrorMessage(DatanodeBlockID blockId, DatanodeDetails d) {
    return String.format("Failed to get block #%s in container #%s from %s",
        blockId.getLocalID(), blockId.getContainerID(), d);
  }

  public static GetBlockResponseProto getBlock(XceiverClientSpi xceiverClient,
      DatanodeBlockID datanodeBlockID,
      Token<? extends TokenIdentifier> token) throws IOException {
    return getBlock(xceiverClient, getValidatorList(), datanodeBlockID, token);
  }

  private static GetBlockResponseProto getBlock(XceiverClientSpi xceiverClient,
      List<CheckedBiFunction> validators,
      ContainerCommandRequestProto.Builder builder,
      DatanodeDetails datanode) throws IOException {
    final ContainerCommandRequestProto request = builder
        .setDatanodeUuid(datanode.getUuidString()).build();
    ContainerCommandResponseProto response =
        xceiverClient.sendCommand(request, validators);
    return response.getGetBlock();
  }

  /**
   * Calls the container protocol to get the length of a committed block.
   *
   * @param xceiverClient client to perform call
   * @param blockID blockId for the Block
   * @param token a token for this block (may be null)
   * @return container protocol getLastCommittedBlockLength response
   * @throws IOException if there is an I/O error while performing the call
   */
  public static ContainerProtos.GetCommittedBlockLengthResponseProto
      getCommittedBlockLength(
      XceiverClientSpi xceiverClient, BlockID blockID,
      Token<OzoneBlockTokenIdentifier> token)
      throws IOException {
    ContainerProtos.GetCommittedBlockLengthRequestProto.Builder
        getBlockLengthRequestBuilder =
        ContainerProtos.GetCommittedBlockLengthRequestProto.newBuilder().
            setBlockID(blockID.getDatanodeBlockIDProtobuf());
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(Type.GetCommittedBlockLength)
            .setContainerID(blockID.getContainerID())
            .setDatanodeUuid(id)
            .setGetCommittedBlockLength(getBlockLengthRequestBuilder);
    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }
    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto response =
        xceiverClient.sendCommand(request, getValidatorList());
    return response.getGetCommittedBlockLength();
  }

  /**
   * Calls the container protocol to put a container block.
   *
   * @param xceiverClient client to perform call
   * @param containerBlockData block data to identify container
   * @param token a token for this block (may be null)
   * @return putBlockResponse
   * @throws IOException if there is an I/O error while performing the call
   */
  public static ContainerProtos.PutBlockResponseProto putBlock(
      XceiverClientSpi xceiverClient, BlockData containerBlockData,
      Token<OzoneBlockTokenIdentifier> token)
      throws IOException {
    PutBlockRequestProto.Builder createBlockRequest =
        PutBlockRequestProto.newBuilder().setBlockData(containerBlockData);
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder().setCmdType(Type.PutBlock)
            .setContainerID(containerBlockData.getBlockID().getContainerID())
            .setDatanodeUuid(id)
            .setPutBlock(createBlockRequest);
    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }
    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto response =
        xceiverClient.sendCommand(request, getValidatorList());
    return response.getPutBlock();
  }

  /**
   * Calls the container protocol to put a container block.
   *
   * @param xceiverClient client to perform call
   * @param containerBlockData block data to identify container
   * @param eof whether this is the last putBlock for the same block
   * @param token a token for this block (may be null)
   * @return putBlockResponse
   * @throws IOException if there is an error while performing the call
   * @throws InterruptedException
   * @throws ExecutionException
   */
  public static XceiverClientReply putBlockAsync(XceiverClientSpi xceiverClient,
      BlockData containerBlockData, boolean eof,
      Token<? extends TokenIdentifier> token)
      throws IOException, InterruptedException, ExecutionException {
    final ContainerCommandRequestProto request = getPutBlockRequest(
        xceiverClient.getPipeline(), containerBlockData, eof, token);
    return xceiverClient.sendCommandAsync(request);
  }

  public static ContainerCommandRequestProto getPutBlockRequest(
      Pipeline pipeline, BlockData containerBlockData, boolean eof,
      Token<? extends TokenIdentifier> token) throws IOException {
    PutBlockRequestProto.Builder createBlockRequest =
        PutBlockRequestProto.newBuilder()
            .setBlockData(containerBlockData)
            .setEof(eof);
    final String id = pipeline.getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder().setCmdType(Type.PutBlock)
            .setContainerID(containerBlockData.getBlockID().getContainerID())
            .setDatanodeUuid(id)
            .setPutBlock(createBlockRequest);
    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }
    return builder.build();
  }

  /**
   * Calls the container protocol to read a chunk.
   *
   * @param xceiverClient client to perform call
   * @param chunk information about chunk to read
   * @param blockID ID of the block
   * @param validators functions to validate the response
   * @param token a token for this block (may be null)
   * @return container protocol read chunk response
   * @throws IOException if there is an I/O error while performing the call
   */
  public static ContainerProtos.ReadChunkResponseProto readChunk(
      XceiverClientSpi xceiverClient, ChunkInfo chunk, BlockID blockID,
      List<CheckedBiFunction> validators,
      Token<? extends TokenIdentifier> token) throws IOException {
    ReadChunkRequestProto.Builder readChunkRequest =
        ReadChunkRequestProto.newBuilder()
            .setBlockID(blockID.getDatanodeBlockIDProtobuf())
            .setChunkData(chunk)
            .setReadChunkVersion(ContainerProtos.ReadChunkVersion.V1);
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder().setCmdType(Type.ReadChunk)
            .setContainerID(blockID.getContainerID())
            .setReadChunk(readChunkRequest);
    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }

    return tryEachDatanode(xceiverClient.getPipeline(),
        d -> readChunk(xceiverClient, chunk, blockID,
            validators, builder, d),
        d -> toErrorMessage(chunk, blockID, d));
  }

  private static ContainerProtos.ReadChunkResponseProto readChunk(
      XceiverClientSpi xceiverClient, ChunkInfo chunk, BlockID blockID,
      List<CheckedBiFunction> validators,
      ContainerCommandRequestProto.Builder builder,
      DatanodeDetails d) throws IOException {
    final ContainerCommandRequestProto request = builder
        .setDatanodeUuid(d.getUuidString()).build();
    ContainerCommandResponseProto reply =
        xceiverClient.sendCommand(request, validators);
    final ReadChunkResponseProto response = reply.getReadChunk();
    final long readLen = getLen(response);
    if (readLen != chunk.getLen()) {
      throw new IOException(toErrorMessage(chunk, blockID, d)
          + ": readLen=" + readLen);
    }
    return response;
  }

  static String toErrorMessage(ChunkInfo chunk, BlockID blockId,
      DatanodeDetails d) {
    return String.format("Failed to read chunk %s (len=%s) %s from %s",
        chunk.getChunkName(), chunk.getLen(), blockId, d);
  }

  static long getLen(ReadChunkResponseProto response) {
    if (response.hasData()) {
      return response.getData().size();
    } else if (response.hasDataBuffers()) {
      return response.getDataBuffers() .getBuffersList().stream()
          .mapToLong(ByteString::size).sum();
    } else {
      return -1;
    }
  }

  /**
   * Calls the container protocol to write a chunk.
   *
   * @param xceiverClient client to perform call
   * @param chunk information about chunk to write
   * @param blockID ID of the block
   * @param data the data of the chunk to write
   * @param token a token for this block (may be null)
   * @throws IOException if there is an error while performing the call
   */
  public static void writeChunk(XceiverClientSpi xceiverClient, ChunkInfo chunk,
      BlockID blockID, ByteString data,
      Token<OzoneBlockTokenIdentifier> token)
      throws IOException {
    WriteChunkRequestProto.Builder writeChunkRequest = WriteChunkRequestProto
        .newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf())
        .setChunkData(chunk)
        .setData(data);
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.WriteChunk)
        .setContainerID(blockID.getContainerID())
        .setDatanodeUuid(id)
        .setWriteChunk(writeChunkRequest);
    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }
    ContainerCommandRequestProto request = builder.build();
    xceiverClient.sendCommand(request, getValidatorList());
  }

  /**
   * Calls the container protocol to write a chunk.
   *
   * @param xceiverClient client to perform call
   * @param chunk information about chunk to write
   * @param blockID ID of the block
   * @param data the data of the chunk to write
   * @param token a token for this block (may be null)
   * @throws IOException if there is an I/O error while performing the call
   */
  public static XceiverClientReply writeChunkAsync(
      XceiverClientSpi xceiverClient, ChunkInfo chunk, BlockID blockID,
      ByteString data, Token<? extends TokenIdentifier> token,
      int replicationIndex
  )
      throws IOException, ExecutionException, InterruptedException {
    WriteChunkRequestProto.Builder writeChunkRequest =
        WriteChunkRequestProto.newBuilder()
            .setBlockID(DatanodeBlockID.newBuilder()
                .setContainerID(blockID.getContainerID())
                .setLocalID(blockID.getLocalID())
                .setBlockCommitSequenceId(blockID.getBlockCommitSequenceId())
                .setReplicaIndex(replicationIndex)
                .build())
            .setChunkData(chunk)
            .setData(data);
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(Type.WriteChunk)
            .setContainerID(blockID.getContainerID())
            .setDatanodeUuid(id)
            .setWriteChunk(writeChunkRequest);
    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }
    ContainerCommandRequestProto request = builder.build();
    return xceiverClient.sendCommandAsync(request);
  }

  /**
   * Allows writing a small file using single RPC. This takes the container
   * name, block name and data to write sends all that data to the container
   * using a single RPC. This API is designed to be used for files which are
   * smaller than 1 MB.
   *
   * @param client - client that communicates with the container.
   * @param blockID - ID of the block
   * @param data - Data to be written into the container.
   * @param token a token for this block (may be null)
   * @return container protocol writeSmallFile response
   * @throws IOException
   */
  public static PutSmallFileResponseProto writeSmallFile(
      XceiverClientSpi client, BlockID blockID, byte[] data,
      Token<OzoneBlockTokenIdentifier> token) throws IOException {

    BlockData containerBlockData =
        BlockData.newBuilder().setBlockID(blockID.getDatanodeBlockIDProtobuf())
            .build();
    PutBlockRequestProto.Builder createBlockRequest =
        PutBlockRequestProto.newBuilder()
            .setBlockData(containerBlockData);

    KeyValue keyValue =
        KeyValue.newBuilder().setKey("OverWriteRequested").setValue("true")
            .build();

    Checksum checksum = new Checksum(ChecksumType.CRC32, 256);
    final ChecksumData checksumData = checksum.computeChecksum(data);
    ChunkInfo chunk =
        ChunkInfo.newBuilder()
            .setChunkName(blockID.getLocalID() + "_chunk")
            .setOffset(0)
            .setLen(data.length)
            .addMetadata(keyValue)
            .setChecksumData(checksumData.getProtoBufMessage())
            .build();

    PutSmallFileRequestProto putSmallFileRequest =
        PutSmallFileRequestProto.newBuilder().setChunkInfo(chunk)
            .setBlock(createBlockRequest).setData(ByteString.copyFrom(data))
            .build();

    String id = client.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder()
            .setCmdType(Type.PutSmallFile)
            .setContainerID(blockID.getContainerID())
            .setDatanodeUuid(id)
            .setPutSmallFile(putSmallFileRequest);
    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }
    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto response =
        client.sendCommand(request, getValidatorList());
    return response.getPutSmallFile();
  }

  /**
   * createRecoveringContainer call that creates a container on the datanode.
   * Currently this is used for EC reconstruction containers. When EC
   * reconstruction coordinator reconstructing the containers, the in progress
   * containers would be created as "RECOVERING" state containers.
   * @param client  - client
   * @param containerID - ID of container
   * @param encodedToken - encodedToken if security is enabled
   * @param replicaIndex - index position of the container replica
   * @throws IOException
   */
  @InterfaceStability.Evolving
  public static void createRecoveringContainer(XceiverClientSpi client,
      long containerID, String encodedToken, int replicaIndex)
      throws IOException {
    createContainer(client, containerID, encodedToken,
        ContainerProtos.ContainerDataProto.State.RECOVERING, replicaIndex);
  }

  /**
   * createContainer call that creates a container on the datanode.
   * @param client  - client
   * @param containerID - ID of container
   * @param encodedToken - encodedToken if security is enabled
   * @throws IOException
   */
  public static void createContainer(XceiverClientSpi client, long containerID,
      String encodedToken) throws IOException {
    createContainer(client, containerID, encodedToken, null, 0);
  }
  /**
   * createContainer call that creates a container on the datanode.
   * @param client  - client
   * @param containerID - ID of container
   * @param encodedToken - encodedToken if security is enabled
   * @param state - state of the container
   * @param replicaIndex - index position of the container replica
   * @throws IOException
   */
  public static void createContainer(XceiverClientSpi client,
      long containerID, String encodedToken,
      ContainerProtos.ContainerDataProto.State state, int replicaIndex)
      throws IOException {
    ContainerProtos.CreateContainerRequestProto.Builder createRequest =
        ContainerProtos.CreateContainerRequestProto.newBuilder();
    createRequest
        .setContainerType(ContainerProtos.ContainerType.KeyValueContainer);
    if (state != null) {
      createRequest.setState(state);
    }
    if (replicaIndex > 0) {
      createRequest.setReplicaIndex(replicaIndex);
    }

    String id = client.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    if (encodedToken != null) {
      request.setEncodedToken(encodedToken);
    }
    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setContainerID(containerID);
    request.setCreateContainer(createRequest.build());
    request.setDatanodeUuid(id);
    client.sendCommand(request.build(), getValidatorList());
  }

  /**
   * Deletes a container from a pipeline.
   *
   * @param client
   * @param force whether or not to forcibly delete the container.
   * @param encodedToken - encodedToken if security is enabled
   * @throws IOException
   */
  public static void deleteContainer(XceiverClientSpi client, long containerID,
      boolean force, String encodedToken) throws IOException {
    ContainerProtos.DeleteContainerRequestProto.Builder deleteRequest =
        ContainerProtos.DeleteContainerRequestProto.newBuilder();
    deleteRequest.setForceDelete(force);
    String id = client.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteContainer);
    request.setContainerID(containerID);
    request.setDeleteContainer(deleteRequest);
    request.setDatanodeUuid(id);
    if (encodedToken != null) {
      request.setEncodedToken(encodedToken);
    }
    client.sendCommand(request.build(), getValidatorList());
  }

  /**
   * Close a container.
   *
   * @param client
   * @param containerID
   * @param encodedToken - encodedToken if security is enabled
   * @throws IOException
   */
  public static void closeContainer(XceiverClientSpi client,
      long containerID, String encodedToken)
      throws IOException {
    String id = client.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(Type.CloseContainer);
    request.setContainerID(containerID);
    request.setCloseContainer(CloseContainerRequestProto.getDefaultInstance());
    request.setDatanodeUuid(id);
    if (encodedToken != null) {
      request.setEncodedToken(encodedToken);
    }
    client.sendCommand(request.build(), getValidatorList());
  }

  /**
   * readContainer call that gets meta data from an existing container.
   *
   * @param client       - client
   * @param encodedToken - encodedToken if security is enabled
   * @throws IOException
   */
  public static ReadContainerResponseProto readContainer(
      XceiverClientSpi client, long containerID, String encodedToken)
      throws IOException {
    String id = client.getPipeline().getFirstNode().getUuidString();

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(Type.ReadContainer);
    request.setContainerID(containerID);
    request.setReadContainer(ReadContainerRequestProto.getDefaultInstance());
    request.setDatanodeUuid(id);
    if (encodedToken != null) {
      request.setEncodedToken(encodedToken);
    }
    ContainerCommandResponseProto response =
        client.sendCommand(request.build(), getValidatorList());

    return response.getReadContainer();
  }

  /**
   * Reads the data given the blockID.
   *
   * @param client
   * @param blockID - ID of the block
   * @param token a token for this block (may be null)
   * @return GetSmallFileResponseProto
   * @throws IOException
   */
  public static GetSmallFileResponseProto readSmallFile(XceiverClientSpi client,
      BlockID blockID,
      Token<OzoneBlockTokenIdentifier> token) throws IOException {
    GetBlockRequestProto.Builder getBlock = GetBlockRequestProto
        .newBuilder()
        .setBlockID(blockID.getDatanodeBlockIDProtobuf());
    ContainerProtos.GetSmallFileRequestProto getSmallFileRequest =
        GetSmallFileRequestProto
            .newBuilder().setBlock(getBlock)
            .setReadChunkVersion(ContainerProtos.ReadChunkVersion.V1)
            .build();
    String id = client.getPipeline().getClosestNode().getUuidString();

    ContainerCommandRequestProto.Builder builder = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.GetSmallFile)
        .setContainerID(blockID.getContainerID())
        .setDatanodeUuid(id)
        .setGetSmallFile(getSmallFileRequest);
    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }
    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto response =
        client.sendCommand(request, getValidatorList());
    return response.getGetSmallFile();
  }

  /**
   * Validates a response from a container protocol call.  Any non-successful
   * return code is mapped to a corresponding exception and thrown.
   *
   * @param response container protocol call response
   * @throws StorageContainerException if the container protocol call failed
   */
  public static void validateContainerResponse(
      ContainerCommandResponseProto response
  ) throws StorageContainerException {
    if (response.getResult() == ContainerProtos.Result.SUCCESS) {
      return;
    } else if (response.getResult()
        == ContainerProtos.Result.BLOCK_NOT_COMMITTED) {
      throw new BlockNotCommittedException(response.getMessage());
    } else if (response.getResult()
        == ContainerProtos.Result.CLOSED_CONTAINER_IO) {
      throw new ContainerNotOpenException(response.getMessage());
    }
    throw new StorageContainerException(
        response.getMessage(), response.getResult());
  }

  public static List<CheckedBiFunction> getValidatorList() {
    return VALIDATORS;
  }

  private static final List<CheckedBiFunction> VALIDATORS = createValidators();

  private static List<CheckedBiFunction> createValidators() {
    CheckedBiFunction<ContainerProtos.ContainerCommandRequestProto,
        ContainerProtos.ContainerCommandResponseProto, IOException>
        validator = (request, response) -> validateContainerResponse(response);
    return Collections.singletonList(validator);
  }

  public static List<CheckedBiFunction> toValidatorList(
      CheckedBiFunction<ContainerCommandRequestProto,
          ContainerCommandResponseProto, IOException> validator) {
    final List<CheckedBiFunction> defaults
        = ContainerProtocolCalls.getValidatorList();
    final List<CheckedBiFunction> validators
        = new ArrayList<>(defaults.size() + 1);
    validators.addAll(defaults);
    validators.add(validator);
    return Collections.unmodifiableList(validators);
  }

  public static HashMap<DatanodeDetails, GetBlockResponseProto>
      getBlockFromAllNodes(
      XceiverClientSpi xceiverClient,
      DatanodeBlockID datanodeBlockID,
      Token<OzoneBlockTokenIdentifier> token)
      throws IOException, InterruptedException {
    GetBlockRequestProto.Builder readBlockRequest = GetBlockRequestProto
            .newBuilder()
            .setBlockID(datanodeBlockID);
    HashMap<DatanodeDetails, GetBlockResponseProto> datanodeToResponseMap
            = new HashMap<>();
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder = ContainerCommandRequestProto
        .newBuilder()
        .setCmdType(Type.GetBlock)
        .setContainerID(datanodeBlockID.getContainerID())
        .setDatanodeUuid(id)
        .setGetBlock(readBlockRequest);
    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }
    ContainerCommandRequestProto request = builder.build();
    Map<DatanodeDetails, ContainerCommandResponseProto> responses =
            xceiverClient.sendCommandOnAllNodes(request);
    for (Map.Entry<DatanodeDetails, ContainerCommandResponseProto> entry:
           responses.entrySet()) {
      datanodeToResponseMap.put(entry.getKey(), entry.getValue().getGetBlock());
    }
    return datanodeToResponseMap;
  }
}
