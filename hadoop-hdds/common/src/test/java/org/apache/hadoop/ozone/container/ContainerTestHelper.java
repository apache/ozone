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

package org.apache.hadoop.ozone.container;

import static org.apache.hadoop.ozone.OzoneConsts.INCREMENTAL_CHUNK_LIST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto.Builder;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.security.token.Token;
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
  public static final String DATANODE_UUID = UUID.randomUUID().toString();
  private static final long DUMMY_CONTAINER_ID = 9999;

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
   * @return ContainerCommandRequestProto
   */
  public static ContainerCommandRequestProto getWriteChunkRequest(
      Pipeline pipeline, BlockID blockID, int datalen)
      throws IOException {
    LOG.trace("writeChunk {} (blockID={}) to pipeline={}",
        datalen, blockID, pipeline);
    return newWriteChunkRequestBuilder(pipeline, blockID, datalen)
        .build();
  }

  public static ContainerCommandRequestProto getListBlockRequest(
      ContainerCommandRequestProto writeChunkRequest) {
    return ContainerCommandRequestProto.newBuilder()
        .setContainerID(writeChunkRequest.getContainerID())
        .setCmdType(ContainerProtos.Type.ListBlock)
        .setDatanodeUuid(writeChunkRequest.getDatanodeUuid())
        .setListBlock(ContainerProtos.ListBlockRequestProto.newBuilder()
            .setCount(10).build())
        .build();
  }

  public static ContainerCommandRequestProto getPutBlockRequest(
      ContainerCommandRequestProto writeChunkRequest) {
    ContainerProtos.BlockData.Builder block =
        ContainerProtos.BlockData.newBuilder()
            .setSize(writeChunkRequest.getWriteChunk().getChunkData().getLen())
            .setBlockID(writeChunkRequest.getWriteChunk().getBlockID())
            .addChunks(writeChunkRequest.getWriteChunk().getChunkData());
    return ContainerCommandRequestProto.newBuilder()
        .setContainerID(writeChunkRequest.getContainerID())
        .setCmdType(ContainerProtos.Type.PutBlock)
        .setDatanodeUuid(writeChunkRequest.getDatanodeUuid())
        .setPutBlock(ContainerProtos.PutBlockRequestProto.newBuilder()
            .setBlockData(block.build())
            .build())
        .build();
  }

  public static Builder newWriteChunkRequestBuilder(Pipeline pipeline,
      BlockID blockID, int datalen) throws IOException {
    ChunkBuffer data = getData(datalen);
    return newWriteChunkRequestBuilder(pipeline, blockID, data, 0);
  }

  public static Builder newWriteChunkRequestBuilder(
      Pipeline pipeline, BlockID blockID, ChunkBuffer data, int seq)
      throws IOException {
    LOG.trace("writeChunk {} (blockID={}) to pipeline={}",
        data.limit(), blockID, pipeline);
    ContainerProtos.WriteChunkRequestProto.Builder writeRequest =
        ContainerProtos.WriteChunkRequestProto
            .newBuilder();

    writeRequest.setBlockID(blockID.getDatanodeBlockIDProtobuf());

    ChunkInfo info = getChunk(blockID.getLocalID(), seq, 0, data.limit());
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
   * Returns a create container command for test purposes. There are a bunch of
   * tests where we need to just send a request and get a reply.
   *
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCreateContainerRequest(
      long containerID, Pipeline pipeline) throws IOException {
    return getCreateContainerRequest(containerID, pipeline, ContainerProtos.ContainerDataProto.State.OPEN);
  }

  /**
   * Returns a create container command for test purposes. There are a bunch of
   * tests where we need to just send a request and get a reply.
   *
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCreateContainerRequest(
      long containerID, Pipeline pipeline, ContainerProtos.ContainerDataProto.State state) throws IOException {
    LOG.trace("addContainer: {}", containerID);
    return getContainerCommandRequestBuilder(containerID, pipeline, state)
        .build();
  }

  private static Builder getContainerCommandRequestBuilder(long containerID,
          Pipeline pipeline, ContainerProtos.ContainerDataProto.State state) throws IOException {
    Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setContainerID(containerID);
    request.setCreateContainer(
        ContainerProtos.CreateContainerRequestProto.getDefaultInstance().toBuilder().setState(state).build());
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

    Builder request = getContainerCommandRequestBuilder(containerID, pipeline,
        ContainerProtos.ContainerDataProto.State.OPEN);
    if (token != null) {
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
    return getPutBlockRequest(pipeline, writeRequest, false);
  }

  public static ContainerCommandRequestProto getPutBlockRequest(
      Pipeline pipeline, ContainerProtos.WriteChunkRequestProto writeRequest, boolean incremental)
      throws IOException {
    return newPutBlockRequestBuilder(pipeline, writeRequest, incremental).build();
  }

  public static Builder newPutBlockRequestBuilder(Pipeline pipeline,
      ContainerProtos.WriteChunkRequestProtoOrBuilder writeRequest)
      throws IOException {
    return newPutBlockRequestBuilder(pipeline, writeRequest, false);
  }

  public static Builder newPutBlockRequestBuilder(Pipeline pipeline,
      ContainerProtos.WriteChunkRequestProtoOrBuilder writeRequest, boolean incremental)
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
    if (incremental) {
      blockData.addMetadata(INCREMENTAL_CHUNK_LIST, "");
    }
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
   * @param response - Response
   */
  public static void verifyGetBlock(ContainerCommandResponseProto response, int expectedChunksCount) {
    assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    assertEquals(expectedChunksCount,
        response.getGetBlock().getBlockData().getChunksCount());
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
   * @param token - container token
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCloseContainer(
      Pipeline pipeline, long containerID, Token<?> token) throws IOException {
    Builder builder = ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.CloseContainer)
        .setContainerID(containerID)
        .setCloseContainer(
            ContainerProtos.CloseContainerRequestProto.getDefaultInstance())
        .setDatanodeUuid(pipeline.getFirstNode().getUuidString());

    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }

    return builder.build();
  }

  public static ContainerCommandRequestProto getCloseContainer(
      Pipeline pipeline, long containerID) throws IOException {
    return getCloseContainer(pipeline, containerID, null);
  }

  /**
   * Returns a delete container request.
   * @param pipeline - pipeline
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getDeleteContainer(
      Pipeline pipeline, long containerID, boolean forceDelete)
      throws IOException {
    Objects.requireNonNull(pipeline, "pipeline == null");
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

  @Nonnull
  public static ContainerProtos.ContainerCommandRequestProto getFinalizeBlockRequest(
      long localID, ContainerInfo container, String uuidString) {
    final ContainerProtos.ContainerCommandRequestProto.Builder builder =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.FinalizeBlock)
            .setContainerID(container.getContainerID())
            .setDatanodeUuid(uuidString);

    final ContainerProtos.DatanodeBlockID blockId =
        ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(container.getContainerID()).setLocalID(localID)
            .setBlockCommitSequenceId(0).build();

    builder.setFinalizeBlock(ContainerProtos.FinalizeBlockRequestProto
        .newBuilder().setBlockID(blockId).build());
    return builder.build();
  }

  public static BlockID getTestBlockID(long containerID) {
    return getTestBlockID(containerID, null);
  }

  public static BlockID getTestBlockID(long containerID, Integer replicaIndex) {
    DatanodeBlockID.Builder datanodeBlockID = DatanodeBlockID.newBuilder().setContainerID(containerID)
        .setLocalID(UniqueId.next());
    if (replicaIndex != null) {
      datanodeBlockID.setReplicaIndex(replicaIndex);
    }
    return BlockID.getFromProtobuf(datanodeBlockID.build());
  }

  public static long getTestContainerID() {
    return UniqueId.next();
  }

  public static String getFixedLengthString(String string, int length) {
    return String.format("%1$" + length + "s", string);
  }

  public static byte[] generateData(int length, boolean random) {
    final byte[] data = new byte[length];
    if (random) {
      ThreadLocalRandom.current().nextBytes(data);
    } else {
      for (int i = 0; i < length; i++) {
        data[i] = (byte) i;
      }
    }
    return data;
  }

  public static ContainerCommandRequestProto getDummyCommandRequestProto(
      ContainerProtos.Type cmdType) {
    return getDummyCommandRequestProto(ClientVersion.CURRENT, cmdType, 0);
  }

  /**
   * Construct fake protobuf messages for various types of requests.
   * This is tedious, however necessary to test. Protobuf classes are final
   * and cannot be mocked by Mockito.
   *
   * @param cmdType type of the container command.
   * @return
   */
  public static ContainerCommandRequestProto getDummyCommandRequestProto(
      ClientVersion clientVersion, ContainerProtos.Type cmdType, int replicaIndex) {
    final Builder builder =
        ContainerCommandRequestProto.newBuilder()
            .setVersion(clientVersion.toProtoValue())
            .setCmdType(cmdType)
            .setContainerID(DUMMY_CONTAINER_ID)
            .setDatanodeUuid(DATANODE_UUID);

    final DatanodeBlockID fakeBlockId =
        DatanodeBlockID.newBuilder()
            .setContainerID(DUMMY_CONTAINER_ID).setLocalID(1).setReplicaIndex(replicaIndex)
            .setBlockCommitSequenceId(101).build();

    final ContainerProtos.ChunkInfo fakeChunkInfo =
        ContainerProtos.ChunkInfo.newBuilder()
            .setChunkName("dummy")
            .setOffset(0)
            .setLen(100)
            .setChecksumData(ContainerProtos.ChecksumData.newBuilder()
                .setBytesPerChecksum(1)
                .setType(ChecksumType.CRC32)
                .build())
            .build();

    switch (cmdType) {
    case ReadContainer:
      builder.setReadContainer(
          ContainerProtos.ReadContainerRequestProto.newBuilder().build());
      break;
    case GetBlock:
      builder.setGetBlock(ContainerProtos.GetBlockRequestProto.newBuilder()
          .setBlockID(fakeBlockId).build());
      break;
    case GetCommittedBlockLength:
      builder.setGetCommittedBlockLength(
          ContainerProtos.GetCommittedBlockLengthRequestProto.newBuilder()
              .setBlockID(fakeBlockId).build());
      break;
    case ReadChunk:
      builder.setReadChunk(ContainerProtos.ReadChunkRequestProto.newBuilder()
          .setBlockID(fakeBlockId).setChunkData(fakeChunkInfo)
          .setReadChunkVersion(ContainerProtos.ReadChunkVersion.V1).build());
      break;
    case GetSmallFile:
      builder
          .setGetSmallFile(ContainerProtos.GetSmallFileRequestProto.newBuilder()
              .setBlock(ContainerProtos.GetBlockRequestProto.newBuilder()
                  .setBlockID(fakeBlockId)
                  .build())
              .build());
      break;
    case FinalizeBlock:
      builder
          .setFinalizeBlock(ContainerProtos
            .FinalizeBlockRequestProto.newBuilder()
            .setBlockID(fakeBlockId).build());
      break;

    default:
      fail("Unhandled request type " + cmdType + " in unit test");
    }

    return builder.build();
  }

  /**
   * Overwrite the file with random bytes.
   */
  public static void corruptFile(File file) {
    try {
      final int length = (int) file.length();

      Path path = file.toPath();
      final byte[] original = IOUtils.readFully(Files.newInputStream(path), length);

      // Corrupt the last byte and middle bytes of the block. The scanner should log this as two errors.
      final byte[] corruptedBytes = Arrays.copyOf(original, length);
      corruptedBytes[length - 1] = (byte) (original[length - 1] << 1);
      corruptedBytes[length / 2] = (byte) (original[length / 2] << 1);

      Files.write(path, corruptedBytes,
          StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);

      assertThat(IOUtils.readFully(Files.newInputStream(path), length))
          .isEqualTo(corruptedBytes)
          .isNotEqualTo(original);
    } catch (IOException ex) {
      // Fail the test.
      throw new UncheckedIOException(ex);
    }
  }

  /**
   * Truncate the file to 0 bytes in length.
   */
  public static void truncateFile(File file) {
    try {
      Files.write(file.toPath(), new byte[0], StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);
      assertEquals(0, file.length());
    } catch (IOException ex) {
      // Fail the test.
      throw new UncheckedIOException(ex);
    }
  }
}
