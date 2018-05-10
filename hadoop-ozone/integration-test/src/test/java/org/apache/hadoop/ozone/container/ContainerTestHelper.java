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

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.ContainerProtos
    .ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.ContainerProtos
    .ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.ContainerProtos.KeyValue;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.hdds.scm.container.common.helpers.PipelineChannel;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Helpers for container tests.
 */
public final class ContainerTestHelper {
  public static final Logger LOG = LoggerFactory.getLogger(
      ContainerTestHelper.class);
  private static Random r = new Random();

  /**
   * Never constructed.
   */
  private ContainerTestHelper() {
  }

  public static void setOzoneLocalStorageRoot(
      Class<?> clazz, OzoneConfiguration conf) {
    String path = GenericTestUtils.getTempPath(clazz.getSimpleName());
    path += conf.getTrimmed(
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT,
        OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT_DEFAULT);
    conf.set(OzoneConfigKeys.OZONE_LOCALSTORAGE_ROOT, path);
  }

  // TODO: mock multi-node pipeline
  /**
   * Create a pipeline with single node replica.
   *
   * @return Pipeline with single node in it.
   * @throws IOException
   */
  public static Pipeline createSingleNodePipeline() throws
      IOException {
    return createPipeline(1);
  }

  public static String createLocalAddress() throws IOException {
    try(ServerSocket s = new ServerSocket(0)) {
      return "127.0.0.1:" + s.getLocalPort();
    }
  }
  public static DatanodeDetails createDatanodeDetails() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    DatanodeDetails datanodeDetails = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID().toString())
        .setIpAddress(socket.getInetAddress().getHostAddress())
        .setHostName(socket.getInetAddress().getHostName())
        .setContainerPort(port)
        .setRatisPort(port)
        .setOzoneRestPort(port)
        .build();

    socket.close();
    return datanodeDetails;
  }

  /**
   * Create a pipeline with single node replica.
   *
   * @return Pipeline with single node in it.
   * @throws IOException
   */
  public static Pipeline createPipeline(int numNodes)
      throws IOException {
    Preconditions.checkArgument(numNodes >= 1);
    final List<DatanodeDetails> ids = new ArrayList<>(numNodes);
    for(int i = 0; i < numNodes; i++) {
      ids.add(createDatanodeDetails());
    }
    return createPipeline(ids);
  }

  public static Pipeline createPipeline(
      Iterable<DatanodeDetails> ids) throws IOException {
    Objects.requireNonNull(ids, "ids == null");
    final Iterator<DatanodeDetails> i = ids.iterator();
    Preconditions.checkArgument(i.hasNext());
    final DatanodeDetails leader = i.next();
    String pipelineName = "TEST-" + UUID.randomUUID().toString().substring(3);
    final PipelineChannel pipelineChannel =
        new PipelineChannel(leader.getUuidString(), LifeCycleState.OPEN,
            ReplicationType.STAND_ALONE, ReplicationFactor.ONE, pipelineName);
    pipelineChannel.addMember(leader);
    for(; i.hasNext();) {
      pipelineChannel.addMember(i.next());
    }
    return new Pipeline(pipelineChannel);
  }

  /**
   * Creates a ChunkInfo for testing.
   *
   * @param keyID - ID of the key
   * @param seqNo - Chunk number.
   * @return ChunkInfo
   * @throws IOException
   */
  public static ChunkInfo getChunk(long keyID, int seqNo, long offset,
      long len) throws IOException {

    ChunkInfo info = new ChunkInfo(String.format("%d.data.%d", keyID,
        seqNo), offset, len);
    return info;
  }

  /**
   * Generates some data of the requested len.
   *
   * @param len - Number of bytes.
   * @return byte array with valid data.
   */
  public static byte[] getData(int len) {
    byte[] data = new byte[len];
    r.nextBytes(data);
    return data;
  }

  /**
   * Computes the hash and sets the value correctly.
   *
   * @param info - chunk info.
   * @param data - data array
   * @throws NoSuchAlgorithmException
   */
  public static void setDataChecksum(ChunkInfo info, byte[] data)
      throws NoSuchAlgorithmException {
    MessageDigest sha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    sha.update(data);
    info.setChecksum(Hex.encodeHexString(sha.digest()));
  }

  /**
   * Returns a writeChunk Request.
   *
   * @param pipeline - A set of machines where this container lives.
   * @param blockID - Block ID of the chunk.
   * @param datalen - Length of data.
   * @return ContainerCommandRequestProto
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  public static ContainerCommandRequestProto getWriteChunkRequest(
      Pipeline pipeline, BlockID blockID, int datalen)
      throws IOException, NoSuchAlgorithmException {
    LOG.trace("writeChunk {} (blockID={}) to pipeline=",
        datalen, blockID, pipeline);
    ContainerProtos.WriteChunkRequestProto.Builder writeRequest =
        ContainerProtos.WriteChunkRequestProto
            .newBuilder();

    Pipeline newPipeline =
        new Pipeline(pipeline.getPipelineChannel());
    writeRequest.setBlockID(blockID.getDatanodeBlockIDProtobuf());

    byte[] data = getData(datalen);
    ChunkInfo info = getChunk(blockID.getLocalID(), 0, 0, datalen);
    setDataChecksum(info, data);

    writeRequest.setChunkData(info.getProtoBufMessage());
    writeRequest.setData(ByteString.copyFrom(data));

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.WriteChunk);
    request.setWriteChunk(writeRequest);
    request.setTraceID(UUID.randomUUID().toString());
    request.setDatanodeUuid(newPipeline.getLeader().getUuidString());

    return request.build();
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
    Pipeline newPipeline =
        new Pipeline(pipeline.getPipelineChannel());
    byte[] data = getData(dataLen);
    ChunkInfo info = getChunk(blockID.getLocalID(), 0, 0, dataLen);
    setDataChecksum(info, data);


    ContainerProtos.PutKeyRequestProto.Builder putRequest =
        ContainerProtos.PutKeyRequestProto.newBuilder();

    KeyData keyData = new KeyData(blockID);
    List<ContainerProtos.ChunkInfo> newList = new LinkedList<>();
    newList.add(info.getProtoBufMessage());
    keyData.setChunks(newList);
    putRequest.setKeyData(keyData.getProtoBufMessage());

    smallFileRequest.setChunkInfo(info.getProtoBufMessage());
    smallFileRequest.setData(ByteString.copyFrom(data));
    smallFileRequest.setKey(putRequest);

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.PutSmallFile);
    request.setPutSmallFile(smallFileRequest);
    request.setTraceID(UUID.randomUUID().toString());
    request.setDatanodeUuid(newPipeline.getLeader().getUuidString());
    return request.build();
  }


  public static ContainerCommandRequestProto getReadSmallFileRequest(
      Pipeline pipeline, ContainerProtos.PutKeyRequestProto putKey)
      throws Exception {
    ContainerProtos.GetSmallFileRequestProto.Builder smallFileRequest =
        ContainerProtos.GetSmallFileRequestProto.newBuilder();
    ContainerCommandRequestProto getKey = getKeyRequest(pipeline, putKey);
    smallFileRequest.setKey(getKey.getGetKey());

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.GetSmallFile);
    request.setGetSmallFile(smallFileRequest);
    request.setTraceID(UUID.randomUUID().toString());
    request.setDatanodeUuid(pipeline.getLeader().getUuidString());
    return request.build();
  }

  /**
   * Returns a read Request.
   *
   * @param pipeline pipeline.
   * @param request writeChunkRequest.
   * @return Request.
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  public static ContainerCommandRequestProto getReadChunkRequest(
      Pipeline pipeline, ContainerProtos.WriteChunkRequestProto request)
      throws IOException, NoSuchAlgorithmException {
    LOG.trace("readChunk blockID={} from pipeline={}",
        request.getBlockID(), pipeline);

    ContainerProtos.ReadChunkRequestProto.Builder readRequest =
        ContainerProtos.ReadChunkRequestProto.newBuilder();
    readRequest.setBlockID(request.getBlockID());
    readRequest.setChunkData(request.getChunkData());

    ContainerCommandRequestProto.Builder newRequest =
        ContainerCommandRequestProto.newBuilder();
    newRequest.setCmdType(ContainerProtos.Type.ReadChunk);
    newRequest.setReadChunk(readRequest);
    newRequest.setTraceID(UUID.randomUUID().toString());
    newRequest.setDatanodeUuid(pipeline.getLeader().getUuidString());
    return newRequest.build();
  }

  /**
   * Returns a delete Request.
   *
   * @param pipeline pipeline.
   * @param writeRequest - write request
   * @return request
   * @throws IOException
   * @throws NoSuchAlgorithmException
   */
  public static ContainerCommandRequestProto getDeleteChunkRequest(
      Pipeline pipeline, ContainerProtos.WriteChunkRequestProto writeRequest)
      throws
      IOException, NoSuchAlgorithmException {
    LOG.trace("deleteChunk blockID={} from pipeline={}",
        writeRequest.getBlockID(), pipeline);

    ContainerProtos.DeleteChunkRequestProto.Builder deleteRequest =
        ContainerProtos.DeleteChunkRequestProto
            .newBuilder();

    deleteRequest.setChunkData(writeRequest.getChunkData());
    deleteRequest.setBlockID(writeRequest.getBlockID());

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteChunk);
    request.setDeleteChunk(deleteRequest);
    request.setTraceID(UUID.randomUUID().toString());
    request.setDatanodeUuid(pipeline.getLeader().getUuidString());
    return request.build();
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

    ContainerProtos.CreateContainerRequestProto.Builder createRequest =
        ContainerProtos.CreateContainerRequestProto
            .newBuilder();
    ContainerProtos.ContainerData.Builder containerData = ContainerProtos
        .ContainerData.newBuilder();
    containerData.setContainerID(containerID);
    createRequest.setContainerData(containerData.build());

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.CreateContainer);
    request.setCreateContainer(createRequest);
    request.setTraceID(UUID.randomUUID().toString());
    request.setDatanodeUuid(pipeline.getLeader().getUuidString());

    return request.build();
  }

  /**
   * Return an update container command for test purposes.
   * Creates a container data based on the given meta data,
   * and request to update an existing container with it.
   *
   * @param containerID
   * @param metaData
   * @return
   * @throws IOException
   */
  public static ContainerCommandRequestProto getUpdateContainerRequest(
      long containerID, Map<String, String> metaData) throws IOException {
    ContainerProtos.UpdateContainerRequestProto.Builder updateRequestBuilder =
        ContainerProtos.UpdateContainerRequestProto.newBuilder();
    ContainerProtos.ContainerData.Builder containerData = ContainerProtos
        .ContainerData.newBuilder();
    containerData.setContainerID(containerID);
    String[] keys = metaData.keySet().toArray(new String[]{});
    for(int i=0; i<keys.length; i++) {
      KeyValue.Builder kvBuilder = KeyValue.newBuilder();
      kvBuilder.setKey(keys[i]);
      kvBuilder.setValue(metaData.get(keys[i]));
      containerData.addMetadata(i, kvBuilder.build());
    }
    Pipeline pipeline =
        ContainerTestHelper.createSingleNodePipeline();
    updateRequestBuilder.setContainerData(containerData.build());

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.UpdateContainer);
    request.setUpdateContainer(updateRequestBuilder.build());
    request.setTraceID(UUID.randomUUID().toString());
    request.setDatanodeUuid(pipeline.getLeader().getUuidString());
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
    ContainerProtos.CreateContainerResponseProto.Builder createResponse =
        ContainerProtos.CreateContainerResponseProto.newBuilder();

    ContainerCommandResponseProto.Builder response =
        ContainerCommandResponseProto.newBuilder();
    response.setCmdType(ContainerProtos.Type.CreateContainer);
    response.setTraceID(request.getTraceID());
    response.setCreateContainer(createResponse.build());
    response.setResult(ContainerProtos.Result.SUCCESS);
    return response.build();
  }

  /**
   * Returns the PutKeyRequest for test purpose.
   * @param pipeline - pipeline.
   * @param writeRequest - Write Chunk Request.
   * @return - Request
   */
  public static ContainerCommandRequestProto getPutKeyRequest(
      Pipeline pipeline, ContainerProtos.WriteChunkRequestProto writeRequest) {
    LOG.trace("putKey: {} to pipeline={}",
        writeRequest.getBlockID());

    ContainerProtos.PutKeyRequestProto.Builder putRequest =
        ContainerProtos.PutKeyRequestProto.newBuilder();

    KeyData keyData = new KeyData(
        BlockID.getFromProtobuf(writeRequest.getBlockID()));
    List<ContainerProtos.ChunkInfo> newList = new LinkedList<>();
    newList.add(writeRequest.getChunkData());
    keyData.setChunks(newList);
    putRequest.setKeyData(keyData.getProtoBufMessage());

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.PutKey);
    request.setPutKey(putRequest);
    request.setTraceID(UUID.randomUUID().toString());
    request.setDatanodeUuid(pipeline.getLeader().getUuidString());
    return request.build();
  }

  /**
   * Gets a GetKeyRequest for test purpose.
   * @param  pipeline - pipeline
   * @param putKeyRequest - putKeyRequest.
   * @return - Request
   * immediately.
   */
  public static ContainerCommandRequestProto getKeyRequest(
      Pipeline pipeline, ContainerProtos.PutKeyRequestProto putKeyRequest) {
    ContainerProtos.DatanodeBlockID blockID =
        putKeyRequest.getKeyData().getBlockID();
    LOG.trace("getKey: blockID={}", blockID);

    ContainerProtos.GetKeyRequestProto.Builder getRequest =
        ContainerProtos.GetKeyRequestProto.newBuilder();
    ContainerProtos.KeyData.Builder keyData = ContainerProtos.KeyData
        .newBuilder();
    keyData.setBlockID(blockID);
    getRequest.setKeyData(keyData);

    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.GetKey);
    request.setGetKey(getRequest);
    request.setTraceID(UUID.randomUUID().toString());
    request.setDatanodeUuid(pipeline.getLeader().getUuidString());
    return request.build();
  }

  /**
   * Verify the response against the request.
   *
   * @param request - Request
   * @param response - Response
   */
  public static void verifyGetKey(ContainerCommandRequestProto request,
      ContainerCommandResponseProto response) {
    Assert.assertEquals(request.getTraceID(), response.getTraceID());
    Assert.assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    ContainerProtos.PutKeyRequestProto putKey = request.getPutKey();
    ContainerProtos.GetKeyRequestProto getKey = request.getGetKey();
    Assert.assertEquals(putKey.getKeyData().getChunksCount(),
        getKey.getKeyData().getChunksCount());
  }

  /**
   * @param pipeline - pipeline.
   * @param putKeyRequest - putKeyRequest.
   * @return - Request
   */
  public static ContainerCommandRequestProto getDeleteKeyRequest(
      Pipeline pipeline, ContainerProtos.PutKeyRequestProto putKeyRequest) {
    LOG.trace("deleteKey: name={}",
        putKeyRequest.getKeyData().getBlockID());
    ContainerProtos.DeleteKeyRequestProto.Builder delRequest =
        ContainerProtos.DeleteKeyRequestProto.newBuilder();
    delRequest.setBlockID(putKeyRequest.getKeyData().getBlockID());
    ContainerCommandRequestProto.Builder request =
        ContainerCommandRequestProto.newBuilder();
    request.setCmdType(ContainerProtos.Type.DeleteKey);
    request.setDeleteKey(delRequest);
    request.setTraceID(UUID.randomUUID().toString());
    request.setDatanodeUuid(pipeline.getLeader().getUuidString());
    return request.build();
  }

  /**
   * Returns a close container request.
   * @param pipeline - pipeline
   * @param containerID - ID of the container.
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getCloseContainer(
      Pipeline pipeline, long containerID) {
    ContainerProtos.CloseContainerRequestProto closeRequest =
        ContainerProtos.CloseContainerRequestProto.newBuilder().
            setContainerID(containerID).build();
    ContainerProtos.ContainerCommandRequestProto cmd =
        ContainerCommandRequestProto.newBuilder().setCmdType(ContainerProtos
            .Type.CloseContainer).setCloseContainer(closeRequest)
            .setTraceID(UUID.randomUUID().toString())
            .setDatanodeUuid(pipeline.getLeader().getUuidString())
            .build();

    return cmd;
  }

  /**
   * Returns a simple request without traceId.
   * @param pipeline - pipeline
   * @param containerID - ID of the container.
   * @return ContainerCommandRequestProto without traceId.
   */
  public static ContainerCommandRequestProto getRequestWithoutTraceId(
      Pipeline pipeline, long containerID) {
    Preconditions.checkNotNull(pipeline);
    ContainerProtos.CloseContainerRequestProto closeRequest =
            ContainerProtos.CloseContainerRequestProto.newBuilder().
                setContainerID(containerID).build();
    ContainerProtos.ContainerCommandRequestProto cmd =
            ContainerCommandRequestProto.newBuilder().setCmdType(ContainerProtos
                    .Type.CloseContainer).setCloseContainer(closeRequest)
                    .setDatanodeUuid(pipeline.getLeader().getUuidString())
                    .build();
    return cmd;
  }

  /**
   * Returns a delete container request.
   * @param pipeline - pipeline
   * @return ContainerCommandRequestProto.
   */
  public static ContainerCommandRequestProto getDeleteContainer(
      Pipeline pipeline, long containerID, boolean forceDelete) {
    Preconditions.checkNotNull(pipeline);
    ContainerProtos.DeleteContainerRequestProto deleteRequest =
        ContainerProtos.DeleteContainerRequestProto.newBuilder().
            setContainerID(containerID).
            setForceDelete(forceDelete).build();
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.DeleteContainer)
        .setDeleteContainer(deleteRequest)
        .setTraceID(UUID.randomUUID().toString())
        .setDatanodeUuid(pipeline.getLeader().getUuidString())
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
    return new BlockID(containerID, Time.getUtcTime());
  }

  public static long getTestContainerID() {
    return Time.getUtcTime();
  }
}
