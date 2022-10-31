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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.hdds.scm.storage.RatisBlockOutputStream;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import com.google.common.annotations.VisibleForTesting;

/**
 * A BlockOutputStreamEntry manages the data writes into the DataNodes.
 * It wraps BlockOutputStreams that are connecting to the DataNodes,
 * and in the meantime accounts the length of data successfully written.
 *
 * The base implementation is handling Ratis-3 writes, with a single stream,
 * but there can be other implementations that are using a different way.
 */
public class BlockOutputStreamEntry extends OutputStream {

  private final OzoneClientConfig config;
  private OutputStream outputStream;
  private BlockID blockID;
  private final String key;
  private final XceiverClientFactory xceiverClientManager;
  private final Pipeline pipeline;
  // total number of bytes that should be written to this stream
  private final long length;
  // the current position of this stream 0 <= currentPosition < length
  private long currentPosition;
  private final Token<OzoneBlockTokenIdentifier> token;

  private BufferPool bufferPool;
  private ContainerClientMetrics clientMetrics;

  @SuppressWarnings({"parameternumber", "squid:S00107"})
  BlockOutputStreamEntry(
      BlockID blockID, String key,
      XceiverClientFactory xceiverClientManager,
      Pipeline pipeline,
      long length,
      BufferPool bufferPool,
      Token<OzoneBlockTokenIdentifier> token,
      OzoneClientConfig config,
      ContainerClientMetrics clientMetrics
  ) {
    this.config = config;
    this.outputStream = null;
    this.blockID = blockID;
    this.key = key;
    this.xceiverClientManager = xceiverClientManager;
    this.pipeline = pipeline;
    this.token = token;
    this.length = length;
    this.currentPosition = 0;
    this.bufferPool = bufferPool;
    this.clientMetrics = clientMetrics;
  }

  /**
   * BlockOutputStream is initialized in this function. This makes sure that
   * xceiverClient initialization is not done during preallocation and only
   * done when data is written.
   * @throws IOException if xceiverClient initialization fails
   */
  void checkStream() throws IOException {
    if (!isInitialized()) {
      createOutputStream();
    }
  }

  /**
   * Creates the outputStreams that are necessary to start the write.
   * Implementors can override this to instantiate multiple streams instead.
   * @throws IOException
   */
  void createOutputStream() throws IOException {
    outputStream = new RatisBlockOutputStream(blockID, xceiverClientManager,
        pipeline, bufferPool, config, token, clientMetrics);
  }

  ContainerClientMetrics getClientMetrics() {
    return clientMetrics;
  }

  @Override
  public void write(int b) throws IOException {
    checkStream();
    getOutputStream().write(b);
    incCurrentPosition();
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkStream();
    getOutputStream().write(b, off, len);
    incCurrentPosition(len);
  }

  void writeOnRetry(long len) throws IOException {
    checkStream();
    BlockOutputStream out = (BlockOutputStream) getOutputStream();
    out.writeOnRetry(len);
    incCurrentPosition(len);
  }

  @Override
  public void flush() throws IOException {
    if (isInitialized()) {
      getOutputStream().flush();
    }
  }

  @Override
  public void close() throws IOException {
    if (isInitialized()) {
      getOutputStream().close();
      // after closing the chunkOutPutStream, blockId would have been
      // reconstructed with updated bcsId
      this.blockID = ((BlockOutputStream) getOutputStream()).getBlockID();
    }
  }

  boolean isClosed() {
    if (isInitialized()) {
      return  ((BlockOutputStream) getOutputStream()).isClosed();
    }
    return false;
  }

  void cleanup(boolean invalidateClient) throws IOException {
    checkStream();
    BlockOutputStream out = (BlockOutputStream) getOutputStream();
    out.cleanup(invalidateClient);
  }

  /**
   * If the underlying BlockOutputStream implements acknowledgement of the
   * writes, this method returns the total number of bytes acknowledged to be
   * stored by the DataNode peers.
   * The default stream implementation returns zero, and if the used stream
   * does not implement acknowledgement, this method returns zero.
   *
   * @return the number of bytes confirmed to by acknowledge by the underlying
   *    BlockOutputStream, or zero if acknowledgment logic is not implemented,
   *    or the entry is not initialized.
   */
  long getTotalAckDataLength() {
    if (isInitialized()) {
      BlockOutputStream out = (BlockOutputStream) getOutputStream();
      blockID = out.getBlockID();
      return out.getTotalAckDataLength();
    } else {
      // For a pre allocated block for which no write has been initiated,
      // the OutputStream will be null here.
      // In such cases, the default blockCommitSequenceId will be 0
      return 0;
    }
  }

  /**
   * Returns the amount of bytes that were attempted to be sent through towards
   * the DataNodes, and the write call succeeded without an exception.
   */
  long getWrittenDataLength() {
    if (isInitialized()) {
      BlockOutputStream out = (BlockOutputStream) getOutputStream();
      return out.getWrittenDataLength();
    } else {
      // For a pre allocated block for which no write has been initiated,
      // the OutputStream will be null here.
      // In such cases, the default blockCommitSequenceId will be 0
      return 0;
    }
  }

  Collection<DatanodeDetails> getFailedServers() {
    if (isInitialized()) {
      BlockOutputStream out = (BlockOutputStream) getOutputStream();
      return out.getFailedServers();
    }
    return Collections.emptyList();
  }

  /**
   * Used to decide if the wrapped output stream is created already or not.
   * @return true if the wrapped stream is already initialized.
   */
  boolean isInitialized() {
    return getOutputStream() != null;
  }

  /**
   * Gets the intended length of the key to be written.
   * @return the length to be written into the key.
   */
  //TODO: this does not belong to here...
  long getLength() {
    return this.length;
  }

  /**
   * Gets the block token that is used to authenticate during the write.
   * @return the block token for writing the data
   */
  Token<OzoneBlockTokenIdentifier> getToken() {
    return this.token;
  }

  /**
   * Gets the amount of bytes remaining from the full write.
   * @return the amount of bytes to still be written to the key
   */
  //TODO: this does not belong to here...
  long getRemaining() {
    return getLength() - getCurrentPosition();
  }

  /**
   * Increases current position by the given length. Used in writes.
   *
   * @param len the amount of bytes to increase position with.
   */
  void incCurrentPosition(long len) {
    currentPosition += len;
  }

  /**
   * Increases current position by one. Used in writes.
   */
  void incCurrentPosition() {
    currentPosition++;
  }

  /**
   * In case of a failure this method can be used to reset the position back to
   * the last position acked by a node before a write failure.
   */
  void resetToAckedPosition() {
    currentPosition = getTotalAckDataLength();
  }

  @VisibleForTesting
  public OutputStream getOutputStream() {
    return this.outputStream;
  }

  @VisibleForTesting
  public BlockID getBlockID() {
    return this.blockID;
  }

  /**
   * During writes a block ID might change as BCSID's are increasing.
   * Implementors might account these changes, and return a different block id
   * here.
   * @param id the last know ID of the block.
   */
  @VisibleForTesting
  protected void updateBlockID(BlockID id) {
    this.blockID = id;
  }

  OzoneClientConfig getConf() {
    return this.config;
  }

  XceiverClientFactory getXceiverClientManager() {
    return this.xceiverClientManager;
  }

  /**
   * Gets the original Pipeline this entry is initialized with.
   * @return the original pipeline
   */
  @VisibleForTesting
  public Pipeline getPipeline() {
    return this.pipeline;
  }

  /**
   * Gets the Pipeline based on which the location report can be sent to the OM.
   * This is necessary, as implementors might use special pipeline information
   * that can be created during commit, but not during initialization,
   * and might need to update some Pipeline information returned in
   * OMKeyLocationInfo.
   * @return
   */
  Pipeline getPipelineForOMLocationReport() {
    return getPipeline();
  }

  long getCurrentPosition() {
    return this.currentPosition;
  }

  BufferPool getBufferPool() {
    return this.bufferPool;
  }

  /**
   * Builder class for ChunkGroupOutputStreamEntry.
   * */
  public static class Builder {

    private BlockID blockID;
    private String key;
    private XceiverClientFactory xceiverClientManager;
    private Pipeline pipeline;
    private long length;
    private BufferPool bufferPool;
    private Token<OzoneBlockTokenIdentifier> token;
    private OzoneClientConfig config;
    private ContainerClientMetrics clientMetrics;

    public Builder setBlockID(BlockID bID) {
      this.blockID = bID;
      return this;
    }

    public Builder setKey(String keys) {
      this.key = keys;
      return this;
    }

    public Builder setXceiverClientManager(
        XceiverClientFactory xClientManager) {
      this.xceiverClientManager = xClientManager;
      return this;
    }

    public Builder setPipeline(Pipeline ppln) {
      this.pipeline = ppln;
      return this;
    }

    public Builder setLength(long len) {
      this.length = len;
      return this;
    }

    public Builder setBufferPool(BufferPool pool) {
      this.bufferPool = pool;
      return this;
    }

    public Builder setConfig(OzoneClientConfig clientConfig) {
      this.config = clientConfig;
      return this;
    }

    public Builder setToken(Token<OzoneBlockTokenIdentifier> bToken) {
      this.token = bToken;
      return this;
    }
    public Builder setClientMetrics(ContainerClientMetrics clientMetrics) {
      this.clientMetrics = clientMetrics;
      return this;
    }

    public BlockOutputStreamEntry build() {
      return new BlockOutputStreamEntry(blockID,
          key,
          xceiverClientManager,
          pipeline,
          length,
          bufferPool,
          token, config, clientMetrics);
    }
  }
}


