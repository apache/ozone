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

package org.apache.hadoop.ozone.container;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto.Builder;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.security.token.Token;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for container tests.
 */
public final class ContainerTestHelper {
  private static final Logger LOG = LoggerFactory.getLogger(
      ContainerTestHelper.class);

  public static final long CONTAINER_MAX_SIZE =
      (long) StorageUnit.GB.toBytes(1);

  /**
   * Never constructed.
   */
  private ContainerTestHelper() {
  }

  // TODO: mock multi-node pipeline

  /**
   * Creates a ChunkInfo for testing.
   *
   * @param keyID - ID of the key
   * @param seqNo - Chunk number.
   * @return ChunkInfo
   */
  public static ChunkInfo getChunk(long keyID, int seqNo, long offset,
      long len) {
    return new ChunkInfo(String.format("%d.data.%d", keyID,
        seqNo), offset, len);
  }

  /**
   * Generates some data of the requested len.
   *
   * @param len - Number of bytes.
   * @return byte array with valid data.
   */
  public static ChunkBuffer getData(int len) {
    byte[] data = new byte[len];
    ThreadLocalRandom.current().nextBytes(data);
    return ChunkBuffer.wrap(ByteBuffer.wrap(data));
  }

  /**
   * Computes the hash and sets the value correctly.
   *
   * @param info - chunk info.
   * @param data - data array
   */
  public static void setDataChecksum(ChunkInfo info, ChunkBuffer data)
      throws OzoneChecksumException {
    Checksum checksum = new Checksum(ChecksumType.CRC32,
        1024 * 1024);
    info.setChecksumData(checksum.computeChecksum(data));
    data.rewind();
  }

  /**
   * Returns a writeChunk Request.
   *
   * @param pipeline - A set of machines where this container lives.
   * @param blockID - Block ID of the chunk.
   * @param datalen - Length of data.
   * @param token - block token.
   * @return ContainerCommandRequestProto
   */
  public static ContainerCommandRequestProto getWriteChunkRequest(
      Pipeline pipeline, BlockID blockID, int datalen, String token)
      throws IOException {
    LOG.trace("writeChunk {} (blockID={}) to pipeline={}",
        datalen, blockID, pipeline);
    return getWriteChunkRequest(pipeline, blockID, datalen, 0, token);
  }

  /**
   * Returns a writeChunk Request.
   *
   * @param pipeline - A set of machines where this container lives.
   * @param blockID - Block ID of the chunk.
   * @param datalen - Length of data.
   * @param token - block token.
   * @return ContainerCommandRequestProto
   */
  public static ContainerCommandRequestProto getWriteChunkRequest(
      Pipeline pipeline, BlockID blockID, int datalen, int seq, String token)
      throws IOException {
    Builder builder = newWriteChunkRequestBuilder(pipeline, blockID, datalen,
        seq);
    if (!Strings.isNullOrEmpty(token)) {
      builder.setEncodedToken(token);
    }
    return builder.build();
  }

  public static Builder newWriteChunkRequestBuilder(
      Pipeline pipeline, BlockID blockID, int datalen, int seq)
      throws IOException {
    LOG.trace("writeChunk {} (blockID={}) to pipeline={}",
        datalen, blockID, pipeline);
    ContainerProtos.WriteChunkRequestProto.Builder writeRequest =
        ContainerProtos.WriteChunkRequestProto
            .newBuilder();

    writeRequest.setBlockID(blockID.getDatanodeBlockIDProtobuf());

    ChunkBuffer data = getData(datalen);
    ChunkInfo info = getChunk(blockID.getLocalID(), seq, 0, datalen);
    setDataChecksum(info, data);

    writeRequest.setChunkData(info.getProtoBufMessage());
    writeRequest.setData(data.toByteString());

    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.WriteChunk);
    request.setContainerID(blockID.getContainerID());
    request.setWriteChunk(writeRequest);
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());

    return request;
  }

  /**
   * Returns PutSmallFile Request that we can send to the container.
   *
   * @param pipeline - Pipeline
   * @param blockID - Block ID of the small file.
   * @param dataLen - Number of bytes in the data
   * @return ContainerCommandRequestProto
   */
  public static ContainerCommandRequestProto getWriteSmallFileRequest(
      Pipeline pipeline, BlockID blockID, int dataLen)
      throws Exception {
    ContainerProtos.PutSmallFileRequestProto.Builder smallFileRequest =
        ContainerProtos.PutSmallFileRequestProto.newBuilder();
    ChunkBuffer data = getData(dataLen);
    ChunkInfo info = getChunk(blockID.getLocalID(), 0, 0, dataLen);
    setDataChecksum(info, data);


    ContainerProtos.PutBlockRequestProto.Builder putRequest =
        ContainerProtos.PutBlockRequestProto.newBuilder();

    BlockData blockData = new BlockData(blockID);
    List<ContainerProtos.ChunkInfo> newList = new LinkedList<>();
    newList.add(info.getProtoBufMessage());
    blockData.setChunks(newList);
    putRequest.setBlockData(blockData.getProtoBufMessage());

    smallFileRequest.setChunkInfo(info.getProtoBufMessage());
    smallFileRequest.setData(data.toByteString());
    smallFileRequest.setBlock(putRequest);

    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.PutSmallFile);
    request.setContainerID(blockID.getContainerID());
    request.setPutSmallFile(smallFileRequest);
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return request.build();
  }


  public static ContainerCommandRequestProto getReadSmallFileRequest(
      Pipeline pipeline, ContainerProtos.PutBlockRequestProto putKey)
      throws Exception {
    ContainerProtos.GetSmallFileRequestProto.Builder smallFileRequest =
        ContainerProtos.GetSmallFileRequestProto.newBuilder();
    ContainerCommandRequestProto getKey = getBlockRequest(pipeline, putKey);
    smallFileRequest.setBlock(getKey.getGetBlock());

    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.GetSmallFile);
    request.setContainerID(getKey.getGetBlock().getBlockID().getContainerID());
    request.setGetSmallFile(smallFileRequest);
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return request.build();
  }

  /**
   * Returns a read Request.
   *
   * @param pipeline pipeline.
   * @param writeChunk writeChunkRequest.
   * @return Request.
   */
  public static ContainerCommandRequestProto getReadChunkRequest(
      Pipeline pipeline, ContainerProtos.WriteChunkRequestProto writeChunk)
      throws IOException {
    return newReadChunkRequestBuilder(pipeline, writeChunk).build();
  }

  public static Builder newReadChunkRequestBuilder(Pipeline pipeline,
      ContainerProtos.WriteChunkRequestProtoOrBuilder writeChunk)
      throws IOException {
    LOG.trace("readChunk blockID={} from pipeline={}",
        writeChunk.getBlockID(), pipeline);

    ContainerProtos.ReadChunkRequestProto.Builder readRequest =
        ContainerProtos.ReadChunkRequestProto.newBuilder();
    readRequest.setBlockID(writeChunk.getBlockID());
    readRequest.setChunkData(writeChunk.getChunkData());
    readRequest.setReadChunkVersion(ContainerProtos.ReadChunkVersion.V1);

    Builder newRequest =
        ContainerCommandRequestProto.newBuilder();
    newRequest.setCmdType(ContainerProtos.Type.ReadChunk);
    newRequest.setContainerID(readRequest.getBlockID().getContainerID());
    newRequest.setReadChunk(readRequest);
    newRequest.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return newRequest;
  }

  /**
   * Returns a delete Request.
   *
   * @param pipeline pipeline.
   * @param writeRequest - write request
   * @return request
   */
  public static ContainerCommandRequestProto getDeleteChunkRequest(
      Pipeline pipeline, ContainerProtos.WriteChunkRequestProto writeRequest)
      throws IOException {
    return newDeleteChunkRequestBuilder(pipeline, writeRequest).build();
  }

  public static Builder newDeleteChunkRequestBuilder(Pipeline pipeline,
      ContainerProtos.WriteChunkRequestProtoOrBuilder writeRequest)
      throws IOException {
    LOG.trace("deleteChunk blockID={} from pipeline={}",
        writeRequest.getBlockID(), pipeline);

    ContainerProtos.DeleteChunkRequestProto.Builder deleteRequest =
        ContainerProtos.DeleteChunkRequestProto
            .newBuilder();

    deleteRequest.setChunkData(writeRequest.getChunkData());
    deleteRequest.setBlockID(writeRequest.getBlockID());

    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteChunk);
    request.setContainerID(writeRequest.getBlockID().getContainerID());
    request.setDeleteChunk(deleteRequest);
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return request;
  }

  /**
   * Returns a create container command for test purposes. There are a bunch of
   * tests where we need to just send a request and get a reply.
   *
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCreateContainerRequest(
      long containerID, Pipeline pipeline) throws IOException {
    LOG.trace("addContainer: {}", containerID);
    return getContainerCommandRequestBuilder(containerID, pipeline).build();
  }

  /**
   * Returns a create container command with token. There are a bunch of
   * tests where we need to just send a request and get a reply.
   *
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCreateContainerRequest(
      long containerID, Pipeline pipeline, Token<?> token) throws IOException {
    LOG.trace("addContainer: {}", containerID);
    return getContainerCommandRequestBuilder(containerID, pipeline)
        .setEncodedToken(token.encodeToUrlString())
        .build();
  }

  private static Builder getContainerCommandRequestBuilder(long containerID,
      Pipeline pipeline) throws IOException {
    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setContainerID(containerID);
    request.setCreateContainer(
        ContainerProtos.CreateContainerRequestProto.getDefaultInstance());
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());

    return request;
  }

  /**
   * Returns a create container command for test purposes. There are a bunch of
   * tests where we need to just send a request and get a reply.
   *
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCreateContainerSecureRequest(
      long containerID, Pipeline pipeline, Token<?> token) throws IOException {
    LOG.trace("addContainer: {}", containerID);

    Builder request = getContainerCommandRequestBuilder(containerID, pipeline);
    if(token != null){
      request.setEncodedToken(token.encodeToUrlString());
    }
    return request.build();
  }

  /**
   * Return an update container command for test purposes.
   * Creates a container data based on the given meta data,
   * and request to update an existing container with it.
   */
  public static ContainerCommandRequestProto getUpdateContainerRequest(
      long containerID, Map<String, String> metaData) throws IOException {
    ContainerProtos.UpdateContainerRequestProto.Builder updateRequestBuilder =
        ContainerProtos.UpdateContainerRequestProto.newBuilder();
    String[] keys = metaData.keySet().toArray(new String[]{});
    for (String key : keys) {
      KeyValue.Builder kvBuilder = KeyValue.newBuilder();
      kvBuilder.setKey(key);
      kvBuilder.setValue(metaData.get(key));
      updateRequestBuilder.addMetadata(kvBuilder.build());
    }
    Pipeline pipeline =
        MockPipeline.createSingleNodePipeline();

    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.UpdateContainer);
    request.setContainerID(containerID);
    request.setUpdateContainer(updateRequestBuilder.build());
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return request.build();
  }

  /**
   * Returns a create container response for test purposes. There are a bunch of
   * tests where we need to just send a request and get a reply.
   *
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandResponseProto
      getCreateContainerResponse(ContainerCommandRequestProto request) {

    ContainerCommandResponseProto.Builder response =
        ContainerCommandResponseProto.newBuilder();
    response.setCmdType(ContainerProtos.Type.CreateContainer);
    response.setTraceID(request.getTraceID());
    response.setCreateContainer(
        ContainerProtos.CreateContainerResponseProto.getDefaultInstance());
    response.setResult(ContainerProtos.Result.SUCCESS);
    return response.build();
  }

  /**
   * Returns the PutBlockRequest for test purpose.
   * @param pipeline - pipeline.
   * @param writeRequest - Write Chunk Request.
   * @return - Request
   */
  public static ContainerCommandRequestProto getPutBlockRequest(
      Pipeline pipeline, ContainerProtos.WriteChunkRequestProto writeRequest)
      throws IOException {
    return getPutBlockRequest(pipeline, null, writeRequest);
  }

  /**
   * Returns the PutBlockRequest for test purpose.
   * @param pipeline - pipeline.
   * @param token - token.
   * @param writeRequest - Write Chunk Request.
   * @return - Request
   */
  public static ContainerCommandRequestProto getPutBlockRequest(
      Pipeline pipeline, String token,
      ContainerProtos.WriteChunkRequestProto writeRequest)
      throws IOException {
    Builder builder = newPutBlockRequestBuilder(pipeline, writeRequest);
    if (!Strings.isNullOrEmpty(token)) {
      builder.setEncodedToken(token);
    }
    return builder.build();
  }

  public static Builder newPutBlockRequestBuilder(Pipeline pipeline,
      ContainerProtos.WriteChunkRequestProtoOrBuilder writeRequest)
      throws IOException {
    LOG.trace("putBlock: {} to pipeline={}",
        writeRequest.getBlockID(), pipeline);

    ContainerProtos.PutBlockRequestProto.Builder putRequest =
        ContainerProtos.PutBlockRequestProto.newBuilder();

    BlockData blockData = new BlockData(
        BlockID.getFromProtobuf(writeRequest.getBlockID()));
    List<ContainerProtos.ChunkInfo> newList = new LinkedList<>();
    newList.add(writeRequest.getChunkData());
    blockData.setChunks(newList);
    blockData.setBlockCommitSequenceId(0);
    putRequest.setBlockData(blockData.getProtoBufMessage());

    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.PutBlock);
    request.setContainerID(blockData.getContainerID());
    request.setPutBlock(putRequest);
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return request;
  }

  /**
   * Gets a GetBlockRequest for test purpose.
   * @param  pipeline - pipeline
   * @param putBlockRequest - putBlockRequest.
   * @return - Request
   * immediately.
   */
  public static ContainerCommandRequestProto getBlockRequest(
      Pipeline pipeline, ContainerProtos.PutBlockRequestProto putBlockRequest)
      throws IOException {
    return newGetBlockRequestBuilder(pipeline, putBlockRequest).build();
  }

  public static Builder newGetBlockRequestBuilder(
      Pipeline pipeline, ContainerProtos.PutBlockRequestProtoOrBuilder putBlock)
      throws IOException {
    DatanodeBlockID blockID = putBlock.getBlockData().getBlockID();

    ContainerProtos.GetBlockRequestProto.Builder getRequest =
        ContainerProtos.GetBlockRequestProto.newBuilder();
    getRequest.setBlockID(blockID);

    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.GetBlock);
    request.setContainerID(blockID.getContainerID());
    request.setGetBlock(getRequest);
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return request;
  }

  /**
   * Verify the response against the request.
   *
   * @param request - Request
   * @param response - Response
   */
  public static void verifyGetBlock(ContainerCommandRequestProto request,
      ContainerCommandResponseProto response, int expectedChunksCount) {
    Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    Assert.assertEquals(expectedChunksCount,
        response.getGetBlock().getBlockData().getChunksCount());
  }

  /**
   * @param pipeline - pipeline.
   * @param putBlockRequest - putBlockRequest.
   * @return - Request
   */
  public static ContainerCommandRequestProto getDeleteBlockRequest(
      Pipeline pipeline, ContainerProtos.PutBlockRequestProto putBlockRequest)
      throws IOException {
    return newDeleteBlockRequestBuilder(pipeline, putBlockRequest).build();
  }

  public static Builder newDeleteBlockRequestBuilder(Pipeline pipeline,
      ContainerProtos.PutBlockRequestProtoOrBuilder putBlockRequest)
      throws IOException {
    DatanodeBlockID blockID = putBlockRequest.getBlockData().getBlockID();
    LOG.trace("deleteBlock: name={}", blockID);
    ContainerProtos.DeleteBlockRequestProto.Builder delRequest =
        ContainerProtos.DeleteBlockRequestProto.newBuilder();
    delRequest.setBlockID(blockID);
    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteBlock);
    request.setContainerID(blockID.getContainerID());
    request.setDeleteBlock(delRequest);
    request.setDatanodeUuid(pipeline.getFirstNode().getUuidString());
    return request;
  }

  public static Builder newGetCommittedBlockLengthBuilder(Pipeline pipeline,
      ContainerProtos.PutBlockRequestProtoOrBuilder putBlock)
      throws IOException {
    DatanodeBlockID blockID = putBlock.getBlockData().getBlockID();

    ContainerProtos.GetCommittedBlockLengthRequestProto.Builder req =
        ContainerProtos.GetCommittedBlockLengthRequestProto.newBuilder()
            .setBlockID(blockID);

    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.GetCommittedBlockLength)
        .setContainerID(blockID.getContainerID())
        .setDatanodeUuid(pipeline.getFirstNode().getUuidString())
        .setGetCommittedBlockLength(req);
  }

  /**
   * Returns a close container request.
   * @param pipeline - pipeline
   * @param containerID - ID of the container.
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCloseContainer(
      Pipeline pipeline, long containerID) throws IOException {
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.CloseContainer)
        .setContainerID(containerID)
        .setCloseContainer(
            ContainerProtos.CloseContainerRequestProto.getDefaultInstance())
        .setDatanodeUuid(pipeline.getFirstNode().getUuidString())
        .build();
  }

  /**
   * Returns a simple request without traceId.
   * @param pipeline - pipeline
   * @param containerID - ID of the container.
   * @return ContainerCommandRequestProto without traceId.
   */
  public static ContainerCommandRequestProto getRequestWithoutTraceId(
      Pipeline pipeline, long containerID) throws IOException {
    Preconditions.checkNotNull(pipeline);
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.CloseContainer)
        .setContainerID(containerID)
        .setCloseContainer(
            ContainerProtos.CloseContainerRequestProto.getDefaultInstance())
        .setDatanodeUuid(pipeline.getFirstNode().getUuidString())
        .build();
  }

  /**
   * Returns a delete container request.
   * @param pipeline - pipeline
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getDeleteContainer(
      Pipeline pipeline, long containerID, boolean forceDelete)
      throws IOException {
    Preconditions.checkNotNull(pipeline);
    ContainerProtos.DeleteContainerRequestProto deleteRequest =
        ContainerProtos.DeleteContainerRequestProto.newBuilder().
            setForceDelete(forceDelete).build();
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.DeleteContainer)
        .setContainerID(containerID)
        .setDeleteContainer(
            ContainerProtos.DeleteContainerRequestProto.getDefaultInstance())
        .setDeleteContainer(deleteRequest)
        .setDatanodeUuid(pipeline.getFirstNode().getUuidString())
        .build();
  }

  private static void sleep(long milliseconds) {
    try {
      Thread.sleep(milliseconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public static BlockID getTestBlockID(long containerID) {
    // Add 2ms delay so that localID based on UtcTime
    // won't collide.
    sleep(2);
    return new BlockID(containerID, HddsUtils.getTime());
  }

  public static long getTestContainerID() {
    return HddsUtils.getTime();
  }

  public static String getFixedLengthString(String string, int length) {
    return String.format("%1$" + length + "s", string);
  }
}
