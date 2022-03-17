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

package org.apache.hadoop.ozone.client.io;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientRatis;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;
import org.apache.hadoop.hdds.scm.storage.StreamBuffer;
import org.apache.hadoop.hdds.scm.storage.StreamRoutingTable;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.RaftRetryFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * SmallFileDataStreamOutput, only used to write requests smaller than ChunkSize
 * <p>
 * TODO : currently not support multi-thread access.
 */
public class SmallFileDataStreamOutput implements ByteBufferStreamOutput {

  public static final Logger LOG =
      LoggerFactory.getLogger(SmallFileDataStreamOutput.class);

  private final AtomicReference<BlockID> blockID;

  private final XceiverClientFactory xceiverClientFactory;
  private XceiverClientRatis xceiverClient;
  private final OzoneClientConfig config;

  private final OzoneManagerProtocol omClient;

  private final OpenKeySession openKeySession;
  private OmKeyLocationInfo keyLocationInfo;
  private final OmKeyArgs keyArgs;
  private final int realFileLen;

  private DataStreamOutput dataStreamOutput;

  private final boolean unsafeByteBufferConversion;

  private final List<StreamBuffer> retryBuffers = new ArrayList<>();
  private long writtenDataLength = 0;
  private long versionID;
  private final Token<OzoneBlockTokenIdentifier> token;

  // error handler
  private final ExcludeList excludeList;
  private final Map<Class<? extends Throwable>, RetryPolicy> retryPolicyMap;
  private int retryCount;

  private final AtomicReference<CompletableFuture<Boolean>> responseFuture =
      new AtomicReference<>();

  private final AtomicBoolean isDataStreamClose = new AtomicBoolean(false);

  private final AtomicReference<ExecutorService> responseExecutor =
      new AtomicReference<>();

  private boolean isDatastreamPipelineMode;

  public SmallFileDataStreamOutput(
      OpenKeySession handler,
      XceiverClientFactory xceiverClientManager,
      OzoneManagerProtocol omClient,
      OzoneClientConfig config,
      boolean unsafeByteBufferConversion
  ) {
    this.xceiverClientFactory = xceiverClientManager;
    this.omClient = omClient;
    this.config = config;
    this.openKeySession = handler;

    this.keyLocationInfo = handler.getKeyInfo().getLatestVersionLocations()
        .getLocationList(handler.getOpenVersion()).get(0);
    this.blockID = new AtomicReference<>(keyLocationInfo.getBlockID());
    this.versionID = keyLocationInfo.getCreateVersion();

    this.unsafeByteBufferConversion = unsafeByteBufferConversion;

    OmKeyInfo info = handler.getKeyInfo();

    this.keyArgs = new OmKeyArgs.Builder().setVolumeName(info.getVolumeName())
        .setBucketName(info.getBucketName()).setKeyName(info.getKeyName())
        .setReplicationConfig(info.getReplicationConfig())
        .setDataSize(info.getDataSize())
        .setIsMultipartKey(false).build();
    this.realFileLen = (int) info.getDataSize();

    this.retryPolicyMap = HddsClientUtils.getRetryPolicyByException(
        config.getMaxRetryCount(), config.getRetryInterval());
    this.retryCount = 0;

    this.excludeList = new ExcludeList();

    this.token = null;

    this.responseFuture.set(new CompletableFuture<>());
    this.responseExecutor.set(Executors.newSingleThreadExecutor());

    this.isDatastreamPipelineMode = config.isDatastreamPipelineMode();
  }

  @VisibleForTesting
  public BlockID getBlockID() {
    return blockID.get();
  }

  @VisibleForTesting
  public OmKeyLocationInfo getKeyLocationInfo() {
    return keyLocationInfo;
  }

  private void allocateNewBlock() throws IOException {
    if (!excludeList.isEmpty()) {
      LOG.info("Allocating block with {}", excludeList);
    }
    Pipeline oldPipeline = this.keyLocationInfo.getPipeline();
    this.keyLocationInfo =
        omClient.allocateBlock(keyArgs, openKeySession.getId(), excludeList);
    BlockID oldBlockID = this.blockID.getAndSet(keyLocationInfo.getBlockID());
    LOG.info("Replace Block {} ({}) to {} ({})", oldBlockID, oldPipeline,
        keyLocationInfo.getBlockID(), keyLocationInfo.getPipeline());
    this.versionID = keyLocationInfo.getCreateVersion();
  }

  @Override
  public void write(ByteBuffer bb) throws IOException {
    if (bb == null) {
      throw new NullPointerException();
    }
    assert writtenDataLength + 1 <= realFileLen;
    retryBuffers.add(new StreamBuffer(bb.duplicate()));
    checkOpen();
    DataStreamOutput out = maybeInitStream();
    writeToContainer(out, bb);
    writtenDataLength++;
  }

  @Override
  public void write(ByteBuffer bb, int off, int len) throws IOException {
    if (bb == null) {
      throw new NullPointerException();
    }

    if ((off < 0) || (off > bb.remaining()) || (len < 0) ||
        ((off + len) > bb.remaining()) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return;
    }
    assert writtenDataLength + len <= realFileLen;
    retryBuffers.add(new StreamBuffer(bb.duplicate(), off, len));
    checkOpen();
    DataStreamOutput out = maybeInitStream();
    writeToContainer(out, bb);
    writtenDataLength += len;
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() throws IOException {
    boolean retry = false;
    while (true) {
      try {
        checkOpen();
        DataStreamOutput out = maybeInitStream();
        if (retry) {
          insideRetry(out);
        }

        ContainerProtos.ContainerCommandRequestProto putSmallFileRequest =
            getPutSmallFileRequest();
        putSmallFileToContainer(putSmallFileRequest, out);

        handleWriteMetaData(out);
      } catch (IOException ee) {
        handleException(ee);
        retry = true;
        continue;
      }
      break;
    }
    cleanup(false);
  }

  private void writeToContainer(DataStreamOutput out, ByteBuffer buf) {
    out.writeAsync(buf).whenCompleteAsync((r, e) -> {
      if (e != null || !r.isSuccess()) {
        String msg =
            "write to stream result is not success, " +
                "failed to putSmallFile into block " + getBlockID();
        if (!this.responseFuture.get().isDone()) {
          this.responseFuture.get()
              .completeExceptionally(new IOException(msg, e));
        }
        LOG.warn(msg);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("write to container success block id: {} write len: {}",
              getBlockID(), buf.capacity());
        }
      }
    }, this.responseExecutor.get());
  }

  private void insideRetry(DataStreamOutput out) {
    for (StreamBuffer retryBuffer : retryBuffers) {
      writeToContainer(out, retryBuffer.duplicate());
    }
  }

  private void handleWriteMetaData(DataStreamOutput out) throws IOException {
    if (out != null) {
      keyArgs.setDataSize(writtenDataLength);
      keyLocationInfo.setLength(writtenDataLength);

      Map<String, String> metadata = keyArgs.getMetadata();
      keyArgs.setMetadata(metadata);

      keyArgs.setLocationInfoList(Collections.singletonList(keyLocationInfo));
      omClient.commitKey(keyArgs, openKeySession.getId());
    } else if (writtenDataLength == 0) {
      keyArgs.setDataSize(0);
      keyLocationInfo.setLength(0);

      Map<String, String> metadata = keyArgs.getMetadata();
      keyArgs.setMetadata(metadata);

      keyArgs.setLocationInfoList(Collections.emptyList());

      omClient.commitKey(keyArgs, openKeySession.getId());
    }
  }

  private void setExceptionAndThrow(IOException ioe) throws IOException {
    throw ioe;
  }

  /**
   * It performs following actions :
   * a. Updates the committed length at datanode for the current stream in
   * datanode.
   * b. Reads the data from the underlying buffer and writes it the next stream.
   *
   * @param exception actual exception that occurred
   * @throws IOException Throws IOException if Write fails
   */
  private void handleException(IOException exception) throws IOException {
    Throwable t = HddsClientUtils.checkForException(exception);
    Preconditions.checkNotNull(t);
    boolean retryFailure = checkForRetryFailure(t);
    boolean containerExclusionException = false;
    if (!retryFailure) {
      containerExclusionException = checkIfContainerToExclude(t);
    }

    long totalSuccessfulFlushedData = 0L;
    long bufferedDataLen = writtenDataLength;

    if (containerExclusionException) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Encountered exception {}. The last committed block length is {}, "
                + "uncommitted data length is {} retry count {}", exception,
            totalSuccessfulFlushedData, bufferedDataLen, retryCount);
      }
      excludeList
          .addConatinerId(ContainerID.valueOf(getBlockID().getContainerID()));
    } else if (xceiverClient != null) {
      LOG.warn(
          "Encountered exception {} on the pipeline {}. "
              + "The last committed block length is {}, "
              + "uncommitted data length is {} retry count {}", exception,
          xceiverClient.getPipeline(), totalSuccessfulFlushedData,
          bufferedDataLen, retryCount);
      excludeList.addPipeline(xceiverClient.getPipeline().getId());
    }
    allocateNewBlock();

    // just clean up the current stream.
    cleanup(retryFailure);

    if (bufferedDataLen > 0) {
      // If the data is still cached in the underlying stream, we need to
      // allocate new block and write this data in the datanode.
      handleRetry(exception);
      // reset the retryCount after handling the exception
      // retryCount = 0;
    }
  }

  private void handleRetry(IOException exception) throws IOException {
    RetryPolicy retryPolicy = retryPolicyMap
        .get(HddsClientUtils.checkForException(exception).getClass());
    if (retryPolicy == null) {
      retryPolicy = retryPolicyMap.get(Exception.class);
    }
    RetryPolicy.RetryAction action = null;
    try {
      action = retryPolicy.shouldRetry(exception, retryCount, 0, true);
    } catch (Exception e) {
      setExceptionAndThrow(new IOException(e));
    }
    if (action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
      String msg = "";
      if (action.reason != null) {
        msg = "Retry request failed. " + action.reason;
        LOG.error(msg, exception);
      }
      setExceptionAndThrow(new IOException(msg, exception));
    }

    // Throw the exception if the thread is interrupted
    if (Thread.currentThread().isInterrupted()) {
      LOG.warn("Interrupted while trying for retry");
      setExceptionAndThrow(exception);
    }
    Preconditions.checkArgument(
        action.action == RetryPolicy.RetryAction.RetryDecision.RETRY);
    if (action.delayMillis > 0) {
      try {
        Thread.sleep(action.delayMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        IOException ioe = (IOException) new InterruptedIOException(
            "Interrupted: action=" + action + ", retry policy=" + retryPolicy)
            .initCause(e);
        setExceptionAndThrow(ioe);
      }
    }
    retryCount++;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Retrying Write request. Already tried {} time(s); " +
          "retry policy is {} ", retryCount, retryPolicy);
    }
  }

  /**
   * Checks if the provided exception signifies retry failure in ratis client.
   * In case of retry failure, ratis client throws RaftRetryFailureException
   * and all succeeding operations are failed with AlreadyClosedException.
   */
  private boolean checkForRetryFailure(Throwable t) {
    return t instanceof RaftRetryFailureException
        || t instanceof AlreadyClosedException;
  }

  // Every container specific exception from datatnode will be seen as
  // StorageContainerException
  private boolean checkIfContainerToExclude(Throwable t) {
    return t instanceof StorageContainerException;
  }

  private void cleanup(boolean invalidateClient) {
    try {
      if (dataStreamOutput != null && !isDataStreamClose.get()) {
        dataStreamOutput.close();
      }
      if (xceiverClientFactory != null && xceiverClient != null) {
        xceiverClientFactory.releaseClient(xceiverClient, invalidateClient);
      }

      responseExecutor.get().shutdownNow();
    } catch (Throwable e) {
      LOG.warn("cleanup error", e);
    } finally {
      dataStreamOutput = null;
      isDataStreamClose.set(false);
      xceiverClient = null;

      responseFuture.set(new CompletableFuture<>());
      responseExecutor.set(Executors.newSingleThreadExecutor());
    }
  }

  private ContainerProtos.ContainerCommandRequestProto getPutSmallFileRequest()
      throws IOException {
    // new checksum
    ByteBuffer checksumBuffer = ByteBuffer.allocate((int) writtenDataLength);
    retryBuffers.forEach(c -> checksumBuffer.put(c.duplicate()));
    checksumBuffer.flip();
    ContainerProtos.ChecksumData checksumData =
        (new Checksum(config.getChecksumType(), config.getBytesPerChecksum()))
            .computeChecksum(checksumBuffer).getProtoBufMessage();

    ContainerProtos.ChunkInfo chunk =
        ContainerProtos.ChunkInfo.newBuilder()
            .setChunkName(getBlockID().getLocalID() + "_chunk_0")
            .setOffset(0)
            .setLen(writtenDataLength)
            .setChecksumData(checksumData)
            .build();

    ContainerProtos.BlockData containerBlockData =
        ContainerProtos.BlockData.newBuilder()
            .setBlockID(getBlockID().getDatanodeBlockIDProtobuf())
            .addChunks(chunk)
            .build();
    ContainerProtos.PutBlockRequestProto.Builder createBlockRequest =
        ContainerProtos.PutBlockRequestProto.newBuilder()
            .setBlockData(containerBlockData);

    ContainerProtos.PutSmallFileRequestProto putSmallFileRequest =
        ContainerProtos.PutSmallFileRequestProto.newBuilder()
            .setChunkInfo(chunk)
            .setBlock(createBlockRequest)
            .build();

    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerProtos.ContainerCommandRequestProto.Builder builder =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.PutSmallFile)
            .setContainerID(getBlockID().getContainerID())
            .setDatanodeUuid(id)
            .setPutSmallFile(putSmallFileRequest);
    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }
    return builder.build();
  }

  private ContainerProtos.ContainerCommandRequestProto
      getStreamInitPutSmallFileRequest() throws IOException {

    ContainerProtos.BlockData containerBlockData =
        ContainerProtos.BlockData.newBuilder()
            .setBlockID(getBlockID().getDatanodeBlockIDProtobuf())
            .setSize(this.keyArgs.getDataSize())
            .build();
    ContainerProtos.PutBlockRequestProto.Builder createBlockRequest =
        ContainerProtos.PutBlockRequestProto.newBuilder()
            .setBlockData(containerBlockData);
    // fake checksum
    Checksum checksum =
        new Checksum(config.getChecksumType(), config.getBytesPerChecksum());
    ContainerProtos.ChunkInfo chunk =
        ContainerProtos.ChunkInfo.newBuilder()
            .setChunkName(getBlockID().getLocalID() + "_chunk_0")
            .setOffset(0)
            .setLen(0)
            .setChecksumData(
                checksum.computeChecksum(new byte[0]).getProtoBufMessage())
            .build();

    ContainerProtos.PutSmallFileRequestProto putSmallFileRequest =
        ContainerProtos.PutSmallFileRequestProto.newBuilder()
            .setChunkInfo(chunk)
            .setBlock(createBlockRequest)
            .build();

    String id = xceiverClient.getPipeline().getFirstNode().getUuidString();
    ContainerProtos.ContainerCommandRequestProto.Builder builder =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.StreamInit)
            .setContainerID(getBlockID().getContainerID())
            .setDatanodeUuid(id)
            .setPutSmallFile(putSmallFileRequest);
    if (token != null) {
      builder.setEncodedToken(token.encodeToUrlString());
    }
    return builder.build();
  }

  private void putSmallFileToContainer(
      ContainerProtos.ContainerCommandRequestProto requestProto,
      DataStreamOutput out)
      throws IOException {
    ByteBuffer buf =
        ContainerCommandRequestMessage.toMessage(requestProto, null)
            .getContent().asReadOnlyByteBuffer();

    out.writeAsync(buf, StandardWriteOption.CLOSE).whenCompleteAsync((r, e) -> {
      isDataStreamClose.set(true);
      if (e != null || !r.isSuccess()) {
        String msg =
            "close stream is not success, Failed to putSmallFile into block " +
                getBlockID();
        if (!responseFuture.get().isDone()) {
          responseFuture.get().completeExceptionally(new IOException(msg, e));
        }
        LOG.warn(msg);
      } else {
        if (!responseFuture.get().isDone()) {
          responseFuture.get().complete(true);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("close stream success, block id: {} metadata len: {}",
              getBlockID(), buf.capacity());
        }

      }
    }, this.responseExecutor.get());

    try {
      responseFuture.get().get();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private DataStreamOutput maybeInitStream() throws IOException {
    if (xceiverClientFactory != null && xceiverClient != null &&
        dataStreamOutput == null) {
      ContainerProtos.ContainerCommandRequestProto streamInitRequest =
          getStreamInitPutSmallFileRequest();
      dataStreamOutput = sendStreamHeader(streamInitRequest);
    }
    return dataStreamOutput;
  }

  private DataStreamOutput sendStreamHeader(
      ContainerProtos.ContainerCommandRequestProto request) {
    ContainerCommandRequestMessage message =
        ContainerCommandRequestMessage.toMessage(request, null);

    if (isDatastreamPipelineMode) {
      return Preconditions.checkNotNull(xceiverClient.getDataStreamApi())
          .stream(message.getContent().asReadOnlyByteBuffer(),
              StreamRoutingTable.getRoutingTable(xceiverClient.getPipeline()));
    } else {
      return Preconditions.checkNotNull(xceiverClient.getDataStreamApi())
          .stream(message.getContent().asReadOnlyByteBuffer());
    }
  }

  private void checkOpen() throws IOException {
    if (xceiverClient == null) {
      this.xceiverClient =
          (XceiverClientRatis) xceiverClientFactory.acquireClient(
              keyLocationInfo.getPipeline());
    }
  }

}
