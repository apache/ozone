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

package org.apache.hadoop.hdds.scm.storage;

import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls.putBlockAsync;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

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
  @SuppressWarnings("checkstyle:ParameterNumber")
  public ECBlockOutputStream(
      BlockID blockID,
      XceiverClientFactory xceiverClientManager,
      Pipeline pipeline,
      BufferPool bufferPool,
      OzoneClientConfig config,
      Token<? extends TokenIdentifier> token,
      ContainerClientMetrics clientMetrics, StreamBufferArgs streamBufferArgs,
      Supplier<ExecutorService> executorServiceSupplier
  ) throws IOException {
    super(blockID, -1, xceiverClientManager,
        pipeline, bufferPool, config, token, clientMetrics, streamBufferArgs, executorServiceSupplier);
    // In EC stream, there will be only one node in pipeline.
    this.datanodeDetails = pipeline.getClosestNode();
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    this.currentChunkRspFuture =
        writeChunkToContainer(
            ChunkBuffer.wrap(ByteBuffer.wrap(b, off, len)));
    updateWrittenDataLength(len);
  }

  public CompletableFuture<ContainerProtos.ContainerCommandResponseProto> write(
      ByteBuffer buff) throws IOException {
    return writeChunkToContainer(ChunkBuffer.wrap(buff));
  }

  public CompletableFuture<ContainerProtos.
      ContainerCommandResponseProto> executePutBlock(boolean close,
      boolean force, long blockGroupLength, BlockData[] blockData)
      throws IOException {

    ECReplicationConfig repConfig = (ECReplicationConfig)
        getPipeline().getReplicationConfig();
    int totalNodes = repConfig.getRequiredNodes();
    int parity = repConfig.getParity();

    //Write checksum only to parity and 1st Replica.
    if (getReplicationIndex() > 1 &&
        getReplicationIndex() <= (totalNodes - parity)) {
      return executePutBlock(close, force, blockGroupLength);
    }

    BlockData checksumBlockData = null;
    BlockID blockID = null;
    //Reverse Traversal as all parity will have checksumBytes
    for (int i = blockData.length - 1; i >= 0; i--) {
      BlockData bd = blockData[i];
      if (bd == null) {
        continue;
      }
      if (blockID == null) {
        // store the BlockID for logging
        blockID = bd.getBlockID();
      }
      List<ChunkInfo> chunks = bd.getChunks();
      if (chunks != null && !chunks.isEmpty()) {
        if (chunks.get(0).hasStripeChecksum()) {
          checksumBlockData = bd;
          break;
        } else {
          ChunkInfo chunk = chunks.get(0);
          LOG.debug("The first chunk in block with index {} does not have stripeChecksum. BlockID: {}, Block " +
                  "size: {}. Chunk length: {}, Chunk offset: {}, hasChecksumData: {}, chunks size: {}.", i,
              bd.getBlockID(), bd.getSize(), chunk.getLen(), chunk.getOffset(), chunk.hasChecksumData(), chunks.size());
        }
      }
    }

    if (checksumBlockData != null) {

      // For the same BlockGroupLength, we need to find the larger value of Block DataSize.
      // This is because we do not send empty chunks to the DataNode, so the larger value is more accurate.
      Map<Long, Optional<BlockData>> maxDataSizeByGroup = Arrays.stream(blockData)
          .filter(Objects::nonNull)
          .collect(Collectors.groupingBy(BlockData::getBlockGroupLength,
          Collectors.maxBy(Comparator.comparingLong(BlockData::getSize))));
      BlockData maxBlockData = maxDataSizeByGroup.get(blockGroupLength).get();

      // When calculating the checksum size,
      // We need to consider both blockGroupLength and the actual size of blockData.
      //
      // We use the smaller value to determine the size of the ChunkList.
      //
      // 1. In most cases, blockGroupLength is equal to the size of blockData.
      // 2. Occasionally, blockData is not fully filled; if a chunk is empty,
      // it is not sent to the DN, resulting in blockData size being smaller than blockGroupLength.
      // 3. In cases with 'dirty data',
      // if an error occurs when writing to the EC-Stripe (e.g., DN reports Container Closed),
      // and the length confirmed with OM is smaller, blockGroupLength may be smaller than blockData size.
      long blockDataSize = Math.min(maxBlockData.getSize(), blockGroupLength);
      int chunkSize = (int) Math.ceil(((double) blockDataSize / repConfig.getEcChunkSize()));
      List<ChunkInfo> checksumBlockDataChunks = checksumBlockData.getChunks();
      if (chunkSize > 0) {
        checksumBlockDataChunks = checksumBlockData.getChunks().subList(0, chunkSize);
      }

      List<ChunkInfo> currentChunks = getContainerBlockData().getChunksList();

      Preconditions.checkArgument(
          currentChunks.size() == checksumBlockDataChunks.size(),
          "The chunk list has " + currentChunks.size()
              + " entries, but the checksum chunks has "
              + checksumBlockDataChunks.size()
              + " entries. They should be equal in size.");
      List<ChunkInfo> newChunkList = new ArrayList<>();

      for (int i = 0; i < currentChunks.size(); i++) {
        ChunkInfo chunkInfo = currentChunks.get(i);
        ChunkInfo checksumChunk = checksumBlockDataChunks.get(i);

        ChunkInfo.Builder builder = ChunkInfo.newBuilder(chunkInfo);

        if (chunkInfo.hasChecksumData()) {
          builder.setStripeChecksum(checksumChunk.getStripeChecksum());
        }

        ChunkInfo newInfo =  builder.build();
        newChunkList.add(newInfo);
      }

      getContainerBlockData().clearChunks();
      getContainerBlockData().addAllChunks(newChunkList);
    } else {
      LOG.warn("Could not find checksum data in any index for blockData with BlockID {}, length {} and " +
          "blockGroupLength {}.", blockID, blockData.length, blockGroupLength);
    }

    return executePutBlock(close, force, blockGroupLength);
  }

  public CompletableFuture<ContainerProtos.
      ContainerCommandResponseProto> executePutBlock(boolean close,
      boolean force, long blockGroupLength, ByteString checksum)
      throws IOException {

    ECReplicationConfig repConfig = (ECReplicationConfig)
        getPipeline().getReplicationConfig();
    int totalNodes = repConfig.getRequiredNodes();
    int parity = repConfig.getParity();

    //Do not update checksum other than parity and 1st Replica.
    if (!(getReplicationIndex() > 1 &&
        getReplicationIndex() <= (totalNodes - parity))) {
      updateChecksum(checksum);
    }
    return executePutBlock(close, force, blockGroupLength);
  }

  public CompletableFuture<ContainerProtos.
      ContainerCommandResponseProto> executePutBlock(boolean close,
      boolean force, long blockGroupLength) throws IOException {
    updateBlockGroupLengthInPutBlockMeta(blockGroupLength);
    return executePutBlock(close, force).thenApply(PutBlockResult::getResponse);
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

  private void updateChecksum(ByteString checksum) {
    int size = getContainerBlockData().getChunksCount();

    if (size > 0) {
      ChunkInfo oldInfo = getContainerBlockData().getChunks(size - 1);
      ChunkInfo newInfo = ChunkInfo.newBuilder(oldInfo)
          .setStripeChecksum(checksum).build();
      getContainerBlockData().removeChunks(size - 1);
      getContainerBlockData().addChunks(newInfo);
    }
  }

  /**
   * @param close whether putBlock is happening as part of closing the stream
   * @param force true if no data was written since most recent putBlock and
   *            stream is being closed
   */
  @Override
  public CompletableFuture<PutBlockResult> executePutBlock(boolean close,
      boolean force) throws IOException {
    checkOpen();

    CompletableFuture<ContainerProtos.
        ContainerCommandResponseProto> flushFuture;
    try {
      ContainerProtos.BlockData blockData = getContainerBlockData().build();
      XceiverClientReply asyncReply =
          putBlockAsync(getXceiverClient(), blockData, close, getTokenString());
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
      throw new IOException(EXCEPTION_MSG + e, e);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      handleInterruptedException(ex, false);
      // never reach.
      return null;
    }
    this.putBlkRspFuture = flushFuture;
    return flushFuture.thenApply(r -> new PutBlockResult(0, r));
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
