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

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls.putBlock;
import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls.writeChunkAsync;

/**
 * Handles the chunk EC writes for an EC internal block.
 */
public class ECBlockOutputStream extends BlockOutputStream{

  private int chunkIndex;
  private CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
      currentChunkRspFuture = null;
  /**
   * Creates a new ECBlockOutputStream.
   *  @param blockID              block ID
   * @param xceiverClientManager client manager that controls client
   * @param pipeline             pipeline where block will be written
   * @param bufferPool           pool of buffers
   */
  public ECBlockOutputStream(BlockID blockID,
      XceiverClientFactory xceiverClientManager, Pipeline pipeline,
      BufferPool bufferPool, OzoneClientConfig config,
      Token<? extends TokenIdentifier> token) throws IOException {
    super(blockID, xceiverClientManager,
        pipeline, bufferPool, config, token);
  }

  public void write(byte[] b, int off, int len) throws IOException {
    currentChunkRspFuture =
        writeChunkToContainer(ChunkBuffer.wrap(ByteBuffer.wrap(b, off, len)));
  }


  public void executePutBlockSync() throws IOException {
    checkOpen();
    try {
      ContainerProtos.BlockData blockData = getContainerBlockData().build();
      putBlock(getXceiverClient(), blockData,
          (Token<OzoneBlockTokenIdentifier>) getToken());
    } catch (IOException e) {
      throw new IOException(EXCEPTION_MSG + e.toString(), e);
    }
  }

  /**
   * Writes buffered data as a new chunk to the container and saves chunk
   * information to be used later in putKey call.
   *
   * @throws IOException if there is an I/O error while performing the call
   * @throws OzoneChecksumException if there is an error while computing
   * checksum
   * @return ContainerCommandResponseProto
   */
  CompletableFuture<ContainerProtos.
      ContainerCommandResponseProto> writeChunkToContainer(
      ChunkBuffer chunk) throws IOException {
    int effectiveChunkSize = chunk.remaining();
    final long offset = getChunkOffset().getAndAdd(effectiveChunkSize);
    final ByteString data =
        chunk.toByteString(getBufferPool().byteStringConversion());
    ChecksumData checksumData = getCheckSum().computeChecksum(chunk);
    ContainerProtos.ChunkInfo chunkInfo = ContainerProtos.ChunkInfo.newBuilder()
        .setChunkName(getBlockID().getLocalID() + "_chunk_" + ++chunkIndex)
        .setOffset(offset).setLen(effectiveChunkSize)
        .setChecksumData(checksumData.getProtoBufMessage()).build();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing chunk {} length {} at offset {}",
          chunkInfo.getChunkName(), effectiveChunkSize, offset);
    }
    try {
      XceiverClientReply asyncReply =
          writeChunkAsync(getXceiverClient(), chunkInfo, getBlockID(), data,
              getToken(), 1);
      CompletableFuture<ContainerProtos.ContainerCommandResponseProto> future =
          asyncReply.getResponse();
      future.thenApplyAsync(e -> {
        try {
          validateResponse(e);
        } catch (IOException sce) {
          future.completeExceptionally(sce);
        }
        return e;
      }, getResponseExecutor()).exceptionally(e -> {
        String msg = "Failed to write chunk " + chunkInfo
            .getChunkName() + " " + "into block " + getBlockID();
        LOG.debug("{}, exception: {}", msg, e.getLocalizedMessage());
        CompletionException ce = new CompletionException(msg, e);
        setIoException(ce);
        throw ce;
      });
      getContainerBlockData().addChunks(chunkInfo);
      return future;
    } catch (IOException | ExecutionException e) {
      throw new IOException(EXCEPTION_MSG + e.toString(), e);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      handleInterruptedException(ex, false);
    }
    return null;
  }

  public CompletableFuture<ContainerProtos.
      ContainerCommandResponseProto> getCurrentChunkResponseFuture() {
    return this.currentChunkRspFuture;
  }
}
