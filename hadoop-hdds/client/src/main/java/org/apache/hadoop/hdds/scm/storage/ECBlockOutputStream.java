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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls.putBlockAsync;

/**
 * Handles the chunk EC writes for an EC internal block.
 */
public class ECBlockOutputStream extends BlockOutputStream {

  private final DatanodeDetails datanodeDetails;
  private CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
      currentChunkRspFuture = null;

  private CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
      putBlkRspFuture = null;
  /**
   * Creates a new ECBlockOutputStream.
   *
   * @param blockID              block ID
   * @param xceiverClientManager client manager that controls client
   * @param pipeline             pipeline where block will be written
   * @param bufferPool           pool of buffers
   */
  public ECBlockOutputStream(
      BlockID blockID,
      XceiverClientFactory xceiverClientManager,
      Pipeline pipeline,
      BufferPool bufferPool,
      OzoneClientConfig config,
      Token<? extends TokenIdentifier> token,
      ContainerClientMetrics clientMetrics
  ) throws IOException {
    super(blockID, xceiverClientManager,
        pipeline, bufferPool, config, token, clientMetrics);
    // In EC stream, there will be only one node in pipeline.
    this.datanodeDetails = pipeline.getClosestNode();
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    this.currentChunkRspFuture =
        writeChunkToContainer(ChunkBuffer.wrap(ByteBuffer.wrap(b, off, len)));
    updateWrittenDataLength(len);
  }

  public CompletableFuture<ContainerProtos.ContainerCommandResponseProto> write(
      ByteBuffer buff) throws IOException {
    return writeChunkToContainer(ChunkBuffer.wrap(buff));
  }

  public CompletableFuture<ContainerProtos.
      ContainerCommandResponseProto> executePutBlock(boolean close,
      boolean force, long blockGroupLength) throws IOException {
    updateBlockGroupLengthInPutBlockMeta(blockGroupLength);
    return executePutBlock(close, force);
  }

  private void updateBlockGroupLengthInPutBlockMeta(final long blockGroupLen) {
    ContainerProtos.KeyValue keyValue = ContainerProtos.KeyValue.newBuilder()
        .setKey(OzoneConsts.BLOCK_GROUP_LEN_KEY_IN_PUT_BLOCK)
        .setValue(String.valueOf(blockGroupLen)).build();
    List<ContainerProtos.KeyValue> metadataList =
        getContainerBlockData().getMetadataList().stream().filter(kv -> !Objects
            .equals(kv.getKey(), OzoneConsts.BLOCK_GROUP_LEN_KEY_IN_PUT_BLOCK))
            .collect(Collectors.toList());
    metadataList.add(keyValue);
    getContainerBlockData().clearMetadata(); // Clears old meta.
    getContainerBlockData().addAllMetadata(metadataList); // Add updated meta.
  }

  /**
   * @param close whether putBlock is happening as part of closing the stream
   * @param force true if no data was written since most recent putBlock and
   *            stream is being closed
   */
  public CompletableFuture<ContainerProtos.
      ContainerCommandResponseProto> executePutBlock(boolean close,
      boolean force) throws IOException {
    checkOpen();

    CompletableFuture<ContainerProtos.
        ContainerCommandResponseProto> flushFuture = null;
    try {
      ContainerProtos.BlockData blockData = getContainerBlockData().build();
      XceiverClientReply asyncReply =
          putBlockAsync(getXceiverClient(), blockData, close, getToken());
      CompletableFuture<ContainerProtos.ContainerCommandResponseProto> future =
          asyncReply.getResponse();
      flushFuture = future.thenApplyAsync(e -> {
        try {
          validateResponse(e);
        } catch (IOException sce) {
          throw new CompletionException(sce);
        }
        // if the ioException is not set, putBlock is successful
        if (getIoException() == null) {
          BlockID responseBlockID = BlockID.getFromProtobuf(
              e.getPutBlock().getCommittedBlockLength().getBlockID());
          Preconditions.checkState(getBlockID().getContainerBlockID()
              .equals(responseBlockID.getContainerBlockID()));
        }
        return e;
      }, getResponseExecutor()).exceptionally(e -> {
        if (LOG.isDebugEnabled()) {
          LOG.debug("putBlock failed for blockID {} with exception {}",
              getBlockID(), e.getLocalizedMessage());
        }
        CompletionException ce =  new CompletionException(e);
        setIoException(ce);
        throw ce;
      });
    } catch (IOException | ExecutionException e) {
      throw new IOException(EXCEPTION_MSG + e.toString(), e);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      handleInterruptedException(ex, false);
    }
    this.putBlkRspFuture = flushFuture;
    return flushFuture;
  }

  @Override
  public void close() throws IOException {
    super.close();
    cleanup(false);
  }

  /**
   * @return The current chunk writer response future.
   */
  public CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
      getCurrentChunkResponseFuture() {
    return this.currentChunkRspFuture;
  }

  /**
   * @return The current chunk putBlock response future.
   */
  public CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
      getCurrentPutBlkResponseFuture() {
    return this.putBlkRspFuture;
  }

  /**
   * Gets the target data node used in the current stream.
   * @return DatanodeDetails
   */
  public DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }

  @Override
  void validateResponse(
      ContainerProtos.ContainerCommandResponseProto responseProto)
      throws IOException {
    try {
      // if the ioException is already set, it means a prev request has failed
      // just throw the exception. The current operation will fail with the
      // original error
      IOException exception = getIoException();
      if (exception != null) {
        return;
      }
      ContainerProtocolCalls.validateContainerResponse(responseProto);
    } catch (IOException sce) {
      setIoException(sce);
    }
  }
}
