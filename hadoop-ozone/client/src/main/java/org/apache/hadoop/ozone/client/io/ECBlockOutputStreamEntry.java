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

package org.apache.hadoop.ozone.client.io;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.ratis.util.Preconditions.assertInstanceOf;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockOutputStream;
import org.apache.hadoop.hdds.scm.storage.ECBlockOutputStream;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ECBlockOutputStreamEntry manages write into EC keys' data block groups.
 * A block group consists of data and parity blocks. For every block we have
 * an internal ECBlockOutputStream instance with a single node pipeline, that
 * is derived from the original EC pipeline.
 */
public class ECBlockOutputStreamEntry extends BlockOutputStreamEntry {

  private static final Logger LOG =
      LoggerFactory.getLogger(ECBlockOutputStreamEntry.class);

  private final ECReplicationConfig replicationConfig;
  private final long length;

  private ECBlockOutputStream[] blockOutputStreams;
  private int currentStreamIdx = 0;
  private long successfulBlkGrpAckedLen;

  ECBlockOutputStreamEntry(Builder b) {
    super(b);
    this.replicationConfig = assertInstanceOf(b.getPipeline().getReplicationConfig(), ECReplicationConfig.class);
    this.length = replicationConfig.getData() * b.getLength();
  }

  @Override
  void checkStream() throws IOException {
    if (!isInitialized()) {
      final ECBlockOutputStream[] streams =
          new ECBlockOutputStream[replicationConfig.getRequiredNodes()];
      Instant timestamp = Instant.now();
      for (int i = currentStreamIdx; i < replicationConfig
          .getRequiredNodes(); i++) {
        List<DatanodeDetails> nodes = getPipeline().getNodes();
        streams[i] =
            new ECBlockOutputStream(getBlockID(), getXceiverClientManager(),
                createSingleECBlockPipeline(getPipeline(), nodes.get(i), i + 1), getBufferPool(),
                getConf(), getToken(), getClientMetrics(), getStreamBufferArgs(), getExecutorServiceSupplier(),
                getVolumeName(), getBucketName(), getKey(), getObjectID(), getParentObjectID(), timestamp);
      }
      blockOutputStreams = streams;
    }
  }

  @Override
  void cleanup(boolean invalidateClient) {
    if (isInitialized()) {
      IOUtils.close(LOG, blockOutputStreams);
      blockOutputStreams = null;
    }
  }

  @Override
  public OutputStream getOutputStream() {
    if (!isInitialized()) {
      return null;
    }
    checkState(blockOutputStreams[currentStreamIdx] != null);
    return blockOutputStreams[currentStreamIdx];
  }

  @Override
  boolean isInitialized() {
    return blockOutputStreams != null;
  }

  @Override
  public long getLength() {
    return length;
  }

  public int getCurrentStreamIdx() {
    return currentStreamIdx;
  }

  public void useNextBlockStream() {
    currentStreamIdx =
        (currentStreamIdx + 1) % replicationConfig.getRequiredNodes();
  }

  public void markFailed(Exception e) {
    if (isInitialized() && blockOutputStreams[currentStreamIdx] != null) {
      blockOutputStreams[currentStreamIdx].setIoException(e);
    }
  }

  public void forceToFirstParityBlock() {
    currentStreamIdx = replicationConfig.getData();
  }

  public void resetToFirstEntry() {
    currentStreamIdx = 0;
  }

  @Override
  void incCurrentPosition() {
    if (isWritingParity()) {
      return;
    }
    super.incCurrentPosition();
  }

  @Override
  void incCurrentPosition(long len) {
    if (isWritingParity()) {
      return;
    }
    super.incCurrentPosition(len);
  }

  @Override
  public void flush() throws IOException {
    if (!isInitialized()) {
      return;
    }
    for (int i = 0;
         i <= currentStreamIdx && i < blockOutputStreams.length; i++) {
      if (blockOutputStreams[i] != null) {
        blockOutputStreams[i].flush();
      }
    }
  }

  @Override
  boolean isClosed() {
    if (!isInitialized()) {
      return false;
    }
    return blockStreams().allMatch(BlockOutputStream::isClosed);
  }

  @Override
  public void close() throws IOException {
    if (!isInitialized()) {
      return;
    }
    for (ECBlockOutputStream stream : blockOutputStreams) {
      if (stream != null) {
        stream.close();
      }
    }
    updateBlockID(underlyingBlockID());
  }

  @Override
  long getTotalAckDataLength() {
    if (!isInitialized()) {
      return 0;
    }
    updateBlockID(underlyingBlockID());

    return this.successfulBlkGrpAckedLen;
  }

  void updateBlockGroupToAckedPosition(long len) {
    if (isWritingParity()) {
      return;
    }
    this.successfulBlkGrpAckedLen = len;
  }

  /**
   * Returns the amount of bytes that were attempted to be sent through towards
   * the DataNodes, and the write call succeeded without an exception.
   * In EC entries the parity writes does not count into this, as the written
   * data length represents the attempts of the classes using the entry, and
   * not the attempts of the entry itself.
   *
   * @return 0 if the stream is not initialized, the amount of data bytes that
   *    were attempted to be written to the entry.
   */
  //TODO: this might become problematic, and should be tested during the
  //      implementation of retries and error handling, as if there is a retry,
  //      then some data might have to be written twice.
  //      This current implementation is an assumption here.
  //      We might need to account the parity bytes written here, or elsewhere.
  @Override
  long getWrittenDataLength() {
    if (!isInitialized()) {
      return 0;
    }
    return dataStreams()
        .mapToLong(BlockOutputStream::getWrittenDataLength)
        .sum();
  }

  @Override
  Collection<DatanodeDetails> getFailedServers() {
    if (!isInitialized()) {
      return Collections.emptyList();
    }

    return blockStreams()
        .flatMap(outputStream -> outputStream.getFailedServers().stream())
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  Pipeline createSingleECBlockPipeline(Pipeline ecPipeline,
      DatanodeDetails node, int replicaIndex) {
    Map<DatanodeDetails, Integer> indiciesForSinglePipeline = Collections.singletonMap(node, replicaIndex);
    return Pipeline.newBuilder()
        .setId(ecPipeline.getId())
        .setReplicationConfig(ecPipeline.getReplicationConfig())
        .setState(ecPipeline.getPipelineState())
        .setNodes(ImmutableList.of(node))
        .setReplicaIndexes(indiciesForSinglePipeline)
        .build();
  }

  void executePutBlock(boolean isClose, long blockGroupLength,
      ByteString checksum) {
    if (!isInitialized()) {
      return;
    }

    for (ECBlockOutputStream stream : blockOutputStreams) {
      if (stream == null) {
        continue;
      }
      try {
        // Set checksum only for 1st node and parity nodes
        stream.executePutBlock(isClose, true, blockGroupLength, checksum);
      } catch (Exception e) {
        stream.setIoException(e);
      }
    }
  }

  private BlockID underlyingBlockID() {
    if (blockOutputStreams[0] == null) {
      return null;
    }
    // blockID is the same for EC blocks inside one block group managed by
    // this entry, so updating based on the first stream, as when we write any
    // data that is surely exists.
    return blockOutputStreams[0].getBlockID();
  }

  public List<ECBlockOutputStream> streamsWithWriteFailure() {
    return getFailedStreams(false);
  }

  public List<ECBlockOutputStream> streamsWithPutBlockFailure() {
    return getFailedStreams(true);
  }

  /**
   * In EC, we will do async write calls for writing data in the scope of a
   * stripe. After every stripe write finishes, use this method to validate the
   * responses of current stripe data writes. This method can also be used to
   * validate the stripe put block responses.
   * @param forPutBlock If true, it will validate the put block response
   *                    futures. It will validate stripe data write response
   *                    futures if false.
   * @return
   */
  private List<ECBlockOutputStream> getFailedStreams(boolean forPutBlock) {
    final Iterator<ECBlockOutputStream> iter = blockStreams().iterator();
    List<ECBlockOutputStream> failedStreams = new ArrayList<>();
    while (iter.hasNext()) {
      final ECBlockOutputStream stream = iter.next();
      if (!forPutBlock && stream.getWrittenDataLength() <= 0) {
        // If we did not write any data to this stream yet, let's not consider
        // for failure checking. But we should do failure checking for putBlock
        // though. In the case of padding stripes, we do send empty put blocks
        // for creating empty containers at DNs ( Refer: HDDS-6794).
        continue;
      }
      CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
          responseFuture = null;
      if (forPutBlock) {
        responseFuture = stream.getCurrentPutBlkResponseFuture();
      } else {
        responseFuture = stream.getCurrentChunkResponseFuture();
      }
      if (isFailed(stream, responseFuture)) {
        failedStreams.add(stream);
      }
    }
    return failedStreams;
  }

  private boolean isFailed(
      ECBlockOutputStream outputStream,
      CompletableFuture<ContainerProtos.
          ContainerCommandResponseProto> chunkWriteResponseFuture) {

    if (chunkWriteResponseFuture == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to reap response from datanode {}",
            outputStream.getDatanodeDetails());
      }
      return true;
    }

    ContainerProtos.ContainerCommandResponseProto containerCommandResponseProto
        = null;
    try {
      containerCommandResponseProto = chunkWriteResponseFuture.get();
    } catch (InterruptedException e) {
      outputStream.setIoException(e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      outputStream.setIoException(e);
    }

    if (outputStream.getIoException() != null) {
      return true;
    }

    if (containerCommandResponseProto == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Empty response from datanode {}",
            outputStream.getDatanodeDetails());
      }
      return true;
    }

    return false;
  }

  private boolean isWritingParity() {
    return currentStreamIdx >= replicationConfig.getData();
  }

  private Stream<ECBlockOutputStream> blockStreams() {
    return isInitialized()
        ? Arrays.stream(blockOutputStreams).filter(Objects::nonNull)
        : Stream.empty();
  }

  private Stream<ECBlockOutputStream> dataStreams() {
    return isInitialized()
        ? Arrays.stream(blockOutputStreams)
            .limit(replicationConfig.getData())
            .filter(Objects::nonNull)
        : Stream.empty();
  }

  public ByteString calculateChecksum() throws IOException {
    if (blockOutputStreams == null) {
      throw new IOException("Block Output Stream is null");
    }

    List<ContainerProtos.ChunkInfo> chunkInfos = new ArrayList<>();
    // First chunk should always have the additional chunks in a partial stripe.
    int currentIdx = blockOutputStreams[0]
        .getContainerBlockData().getChunksCount();
    for (ECBlockOutputStream stream: blockOutputStreams) {
      if (stream.getContainerBlockData().getChunksCount() > currentIdx - 1) {
        chunkInfos.add(stream.getContainerBlockData()
            .getChunksList().get(currentIdx - 1));
      }
    }

    ByteString checksum = ByteString.EMPTY;
    for (ContainerProtos.ChunkInfo info : chunkInfos) {
      for (ByteString byteString : info.getChecksumData().getChecksumsList()) {
        checksum = checksum.concat(byteString);
      }
    }

    return checksum;
  }

  /**
   * Builder class for ChunkGroupOutputStreamEntry.
   * */
  public static class Builder extends BlockOutputStreamEntry.Builder {
    @Override
    public ECBlockOutputStreamEntry build() {
      return new ECBlockOutputStreamEntry(this);
    }
  }
}
