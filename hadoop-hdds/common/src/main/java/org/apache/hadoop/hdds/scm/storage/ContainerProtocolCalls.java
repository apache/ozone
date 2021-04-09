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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutBlockRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutSmallFileRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.PutSmallFileResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadChunkRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.BlockNotCommittedException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerNotOpenException;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.security.token.Token;

import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/**
 * Implementation of all container protocol calls performed by Container
 * clients.
 */
public final class ContainerProtocolCalls  {

  /**
   * There is no need to instantiate this class.
   */
  private ContainerProtocolCalls() {
  }

  /**
   * Calls the container protocol to get a container block.
   *
   * @param xceiverClient client to perform call
   * @param datanodeBlockID blockID to identify container
   * @param token a token for this block (may be null)
   * @return container protocol get block response
   * @throws IOException if there is an I/O error while performing the call
   */
  public static GetBlockResponseProto getBlock(XceiverClientSpi xceiverClient,
      DatanodeBlockID datanodeBlockID,
      Token<? extends TokenIdentifier> token) throws IOException {
    GetBlockRequestProto.Builder readBlockRequest = GetBlockRequestProto
        .newBuilder()
        .setBlockID(datanodeBlockID);
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
    ContainerCommandResponseProto response =
        xceiverClient.sendCommand(request, getValidatorList());
    if (ContainerProtos.Result.CONTAINER_NOT_FOUND.equals(
        response.getResult())) {
      throw new ContainerNotFoundException(response.getMessage());
    }
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
  public static XceiverClientReply putBlockAsync(
      XceiverClientSpi xceiverClient, BlockData containerBlockData, boolean eof,
      Token<? extends TokenIdentifier> token)
      throws IOException, InterruptedException, ExecutionException {
    PutBlockRequestProto.Builder createBlockRequest =
        PutBlockRequestProto.newBuilder()
            .setBlockData(containerBlockData)
            .setEof(eof);
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
    return xceiverClient.sendCommandAsync(request);
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
    String id = xceiverClient.getPipeline().getClosestNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder().setCmdType(Type.ReadChunk)
            .setContainerID(blockID.getContainerID())
            .setDatanodeUuid(id).setReadChunk(readChunkRequest);
    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }
    ContainerCommandRequestProto request = builder.build();
    ContainerCommandResponseProto reply =
        xceiverClient.sendCommand(request, validators);
    return reply.getReadChunk();
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
      ByteString data, Token<? extends TokenIdentifier> token)
      throws IOException, ExecutionException, InterruptedException {
    WriteChunkRequestProto.Builder writeChunkRequest =
        WriteChunkRequestProto.newBuilder()
            .setBlockID(blockID.getDatanodeBlockIDProtobuf())
            .setChunkData(chunk).setData(data);
    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerCommandRequestProto.Builder builder =
        ContainerCommandRequestProto.newBuilder().setCmdType(Type.WriteChunk)
            .setContainerID(blockID.getContainerID())
            .setDatanodeUuid(id).setWriteChunk(writeChunkRequest);
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
   * createContainer call that creates a container on the datanode.
   * @param client  - client
   * @param containerID - ID of container
   * @param encodedToken - encodedToken if security is enabled
   * @throws IOException
   */
  public static void createContainer(XceiverClientSpi client, long containerID,
      String encodedToken) throws IOException {
    ContainerProtos.CreateContainerRequestProto.Builder createRequest =
        ContainerProtos.CreateContainerRequestProto
            .newBuilder();
    createRequest.setContainerType(ContainerProtos.ContainerType
        .KeyValueContainer);

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
    if(encodedToken != null) {
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
    if(encodedToken != null) {
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
    List<CheckedBiFunction> validators = new ArrayList<>(1);
    CheckedBiFunction<ContainerProtos.ContainerCommandRequestProto,
        ContainerProtos.ContainerCommandResponseProto, IOException>
        validator = (request, response) -> validateContainerResponse(response);
    validators.add(validator);
    return validators;
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
    for(Map.Entry<DatanodeDetails, ContainerCommandResponseProto> entry:
           responses.entrySet()){
      datanodeToResponseMap.put(entry.getKey(), entry.getValue().getGetBlock());
    }
    return datanodeToResponseMap;
  }
}
