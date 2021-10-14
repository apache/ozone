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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls.putBlockAsync;

/**
 * Handles the chunk EC writes for an EC internal block.
 */
public class ECBlockOutputStream extends BlockOutputStream{

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
      Token<? extends TokenIdentifier> token
  ) throws IOException {
    super(blockID, xceiverClientManager,
        pipeline, bufferPool, config, token);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    writeChunkToContainer(ChunkBuffer.wrap(ByteBuffer.wrap(b, off, len)));
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
    return flushFuture;
  }

  @Override
  public void close() throws IOException {
    super.close();
    cleanup(false);
  }
}
