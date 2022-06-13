/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.ec.reconstruction;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.hdds.scm.storage.ECBlockOutputStream;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactory;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactoryImpl;
import org.apache.hadoop.ozone.client.io.ECBlockInputStreamProxy;
import org.apache.hadoop.ozone.client.io.ECBlockReconstructedStripeInputStream;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The Coordinator implements the main flow of reconstructing
 * missing container replicas.
 * <p>
 * For a container reconstruction task, the main flow is:
 * - ListBlock from all healthy replicas
 * - calculate effective block group len for all blocks
 * - for each block
 * - build a ReconstructInputStream to read healthy chunks
 * - build a ECBlockOutputStream to write out decoded chunks
 * - for each stripe
 * - use ReconstructInputStream.readStripe to decode missing chunks
 * - use ECBlockOutputStream.write to write decoded chunks to TargetDNs
 * - PutBlock
 * - CloseContainer
 */
public class ECReconstructionCoordinator implements Closeable {

  static final Logger LOG =
      LoggerFactory.getLogger(ECReconstructionCoordinator.class);

  private static final int EC_RECONSTRUCT_STRIPE_READ_POOL_MIN_SIZE = 3;

  private final ECContainerOperationClient containerOperationClient;

  private final ConfigurationSource config;

  private final ByteBufferPool byteBufferPool;

  private ExecutorService ecReconstructExecutor;

  private BlockInputStreamFactory blockInputStreamFactory;

  public ECReconstructionCoordinator(ECContainerOperationClient containerClient,
      ConfigurationSource conf, ByteBufferPool byteBufferPool,
      ExecutorService reconstructExecutor,
      BlockInputStreamFactory streamFactory) {
    this.containerOperationClient = containerClient;
    this.config = conf;
    this.byteBufferPool = byteBufferPool;
    this.blockInputStreamFactory = streamFactory;
    this.ecReconstructExecutor = reconstructExecutor;
  }

  public ECReconstructionCoordinator(ConfigurationSource conf)
      throws IOException {
    this(new ECContainerOperationClient(conf), conf,
        new ElasticByteBufferPool(), null, null);
    this.ecReconstructExecutor =
        new ThreadPoolExecutor(EC_RECONSTRUCT_STRIPE_READ_POOL_MIN_SIZE,
            config.getObject(OzoneClientConfig.class)
                .getEcReconstructStripeReadPoolLimit(), 60, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadFactoryBuilder()
            .setNameFormat("ec-reconstruct-reader-TID-%d").build(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    this.blockInputStreamFactory = BlockInputStreamFactoryImpl
        .getInstance(byteBufferPool, () -> ecReconstructExecutor);
  }

  public void reconstructECContainerGroup(long containerID,
      ECReplicationConfig repConfig,
      SortedMap<Integer, DatanodeDetails> sourceNodeMap,
      SortedMap<Integer, DatanodeDetails> targetNodeMap) throws IOException {

    Pipeline pipeline = rebuildInputPipeline(repConfig, sourceNodeMap);

    SortedMap<Long, BlockData[]> blockDataMap =
        getBlockDataMap(containerID, repConfig, sourceNodeMap);

    SortedMap<Long, BlockLocationInfo> blockLocationInfoMap =
        calcBlockLocationInfoMap(containerID, blockDataMap, pipeline);

    // 1. create target recovering containers.
    Set<Map.Entry<Integer, DatanodeDetails>> targetIndexDns =
        targetNodeMap.entrySet();
    Iterator<Map.Entry<Integer, DatanodeDetails>> iterator =
        targetIndexDns.iterator();
    while (iterator.hasNext()) {
      DatanodeDetails dn = iterator.next().getValue();
      this.containerOperationClient
          .createRecoveringContainer(containerID, dn, repConfig, null);
    }

    // 2. Reconstruct and transfer to targets
    for (BlockLocationInfo blockLocationInfo : blockLocationInfoMap.values()) {
      reconstructECBlockGroup(blockLocationInfo, repConfig, targetNodeMap);
    }

    // 3. Close containers
    iterator = targetIndexDns.iterator();
    while (iterator.hasNext()) {
      DatanodeDetails dn = iterator.next().getValue();
      this.containerOperationClient
          .closeContainer(containerID, dn, repConfig, null);
    }

  }

  void reconstructECBlockGroup(BlockLocationInfo blockLocationInfo,
      ECReplicationConfig repConfig,
      SortedMap<Integer, DatanodeDetails> targetMap)
      throws IOException {
    long safeBlockGroupLength = blockLocationInfo.getLength();
    List<Integer> missingContainerIndexes =
        targetMap.keySet().stream().collect(Collectors.toList());

    // calculate the real missing block indexes
    int dataLocs = ECBlockInputStreamProxy
        .expectedDataLocations(repConfig, safeBlockGroupLength);
    List<Integer> toReconstructIndexes = new ArrayList<>();
    for (int i = 0; i < missingContainerIndexes.size(); i++) {
      Integer index = missingContainerIndexes.get(i);
      if (index <= dataLocs || index > repConfig.getData()) {
        toReconstructIndexes.add(index);
      }
      // else padded indexes.
    }

    // Looks like we don't need to reconstruct any missing blocks in this block
    // group. The reason for this should be block group had only padding blocks
    // in the missing locations.
    if (toReconstructIndexes.size() == 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping the reconstruction for the block: "
            + blockLocationInfo.getBlockID() + ". In the missing locations: "
            + missingContainerIndexes
            + ", this block group has only padded blocks.");
      }
      return;
    }

    try (ECBlockReconstructedStripeInputStream sis
        = new ECBlockReconstructedStripeInputStream(
        repConfig, blockLocationInfo, true,
        this.containerOperationClient.getXceiverClientManager(), null,
        this.blockInputStreamFactory, byteBufferPool,
        this.ecReconstructExecutor)) {

      ECBlockOutputStream[] targetBlockStreams =
          new ECBlockOutputStream[toReconstructIndexes.size()];
      ByteBuffer[] bufs = new ByteBuffer[toReconstructIndexes.size()];
      for (int i = 0; i < toReconstructIndexes.size(); i++) {
        OzoneClientConfig configuration = new OzoneClientConfig();
        // TODO: Let's avoid unnecessary bufferPool creation for
        BufferPool bufferPool =
            new BufferPool(configuration.getStreamBufferSize(),
                (int) (configuration.getStreamBufferMaxSize() / configuration
                    .getStreamBufferSize()),
                ByteStringConversion.createByteBufferConversion(false));
        targetBlockStreams[i] =
            new ECBlockOutputStream(blockLocationInfo.getBlockID(),
                this.containerOperationClient.getXceiverClientManager(),
                Pipeline.newBuilder().setId(PipelineID.valueOf(
                    targetMap.get(toReconstructIndexes.get(i)).getUuid()))
                    .setReplicationConfig(repConfig).setNodes(ImmutableList
                    .of(targetMap.get(toReconstructIndexes.get(i))))
                    .setState(Pipeline.PipelineState.CLOSED).build(),
                bufferPool, configuration, null);
        bufs[i] = byteBufferPool.getBuffer(false, repConfig.getEcChunkSize());
        // Make sure it's clean. Don't want to reuse the erroneously returned
        // buffers from the pool.
        bufs[i].clear();
      }

      sis.setRecoveryIndexes(toReconstructIndexes.stream().map(i -> (i - 1))
          .collect(Collectors.toSet()));
      long length = safeBlockGroupLength;
      while (length > 0) {
        int readLen = sis.recoverChunks(bufs);
        // TODO: can be submitted in parallel
        for (int i = 0; i < bufs.length; i++) {
          targetBlockStreams[i].write(bufs[i]);
          if (isFailed(targetBlockStreams[i],
              targetBlockStreams[i].getCurrentChunkResponseFuture())) {
            // If one chunk response failed, we should retry.
            // Even after retries if it failed, we should declare the
            // reconstruction as failed.
            // For now, let's throw the exception.
            throw new IOException(
                "Chunk write failed at the new target node: "
                    + targetBlockStreams[i].getDatanodeDetails()
                    + ". Aborting the reconstruction process.");
          }
          bufs[i].clear();
        }
        length -= readLen;
      }

      for (int i = 0; i < targetBlockStreams.length; i++) {
        try {
          targetBlockStreams[i]
              .executePutBlock(true, true, blockLocationInfo.getLength());
          if (isFailed(targetBlockStreams[i],
              targetBlockStreams[i].getCurrentPutBlkResponseFuture())) {
            // If one chunk response failed, we should retry.
            // Even after retries if it failed, we should declare the
            // reconstruction as failed.
            // For now, let's throw the exception.
            throw new IOException(
                "Chunk write failed at the new target node: "
                    + targetBlockStreams[i].getDatanodeDetails()
                    + ". Aborting the reconstruction process.");
          }
        } finally {
          byteBufferPool.putBuffer(bufs[i]);
          targetBlockStreams[i].close();
        }
      }
    }
  }

  private boolean isFailed(ECBlockOutputStream outputStream,
      CompletableFuture<ContainerProtos.
          ContainerCommandResponseProto> chunkWriteResponseFuture) {
    if (chunkWriteResponseFuture == null) {
      return true;
    }

    ContainerProtos.ContainerCommandResponseProto
        containerCommandResponseProto = null;
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
      return true;
    }

    return false;
  }

  SortedMap<Long, BlockLocationInfo> calcBlockLocationInfoMap(long containerID,
      SortedMap<Long, BlockData[]> blockDataMap, Pipeline pipeline) {

    SortedMap<Long, BlockLocationInfo> blockInfoMap = new TreeMap<>();

    for (Map.Entry<Long, BlockData[]> entry : blockDataMap.entrySet()) {
      Long localID = entry.getKey();
      BlockData[] blockGroup = entry.getValue();

      long blockGroupLen = calcEffectiveBlockGroupLen(blockGroup,
          pipeline.getReplicationConfig().getRequiredNodes());
      if (blockGroupLen > 0) {
        BlockLocationInfo blockLocationInfo = new BlockLocationInfo.Builder()
            .setBlockID(new BlockID(containerID, localID))
            .setLength(blockGroupLen).setPipeline(pipeline).build();
        blockInfoMap.put(localID, blockLocationInfo);
      }
    }
    return blockInfoMap;
  }

  @Override
  public void close() throws IOException {
    if (containerOperationClient != null) {
      containerOperationClient.close();
    }
  }

  private Pipeline rebuildInputPipeline(ECReplicationConfig repConfig,
      SortedMap<Integer, DatanodeDetails> sourceNodeMap) {

    List<DatanodeDetails> nodes = new ArrayList<>(sourceNodeMap.values());
    Map<DatanodeDetails, Integer> dnVsIndex = new HashMap<>();

    Iterator<Map.Entry<Integer, DatanodeDetails>> iterator =
        sourceNodeMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Integer, DatanodeDetails> next = iterator.next();
      Integer key = next.getKey();
      DatanodeDetails value = next.getValue();
      dnVsIndex.put(value, key);
    }

    return Pipeline.newBuilder().setId(PipelineID.randomId())
        .setReplicationConfig(repConfig).setNodes(nodes)
        .setReplicaIndexes(dnVsIndex).setState(Pipeline.PipelineState.CLOSED)
        .build();
  }

  private SortedMap<Long, BlockData[]> getBlockDataMap(long containerID,
      ECReplicationConfig repConfig,
      Map<Integer, DatanodeDetails> sourceNodeMap) throws IOException {

    SortedMap<Long, BlockData[]> resultMap = new TreeMap<>();

    Iterator<Map.Entry<Integer, DatanodeDetails>> iterator =
        sourceNodeMap.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<Integer, DatanodeDetails> next = iterator.next();
      Integer index = next.getKey();
      DatanodeDetails dn = next.getValue();

      BlockData[] blockDataArr =
          containerOperationClient.listBlock(containerID, dn, repConfig, null);

      for (BlockData blockData : blockDataArr) {
        BlockID blockID = blockData.getBlockID();
        BlockData[] blkDataArr = resultMap.getOrDefault(blockData.getLocalID(),
            new BlockData[repConfig.getRequiredNodes()]);
        blkDataArr[index - 1] = blockData;
        resultMap.put(blockID.getLocalID(), blkDataArr);
      }
    }
    return resultMap;
  }

  /**
   * Get the effective length of each block group.
   * We can not be absolutely accurate when there is a failed stripe
   * in this block since the failed cells could be missing, and
   * we can not tell from the healthy cells whether the last stripe
   * is failed or not. But in such case we at most recover one extra
   * stripe for this block which does not confuse the client data view.
   *
   * @param blockGroup
   * @param replicaCount
   * @return
   */
  private long calcEffectiveBlockGroupLen(BlockData[] blockGroup,
      int replicaCount) {
    Preconditions.checkState(blockGroup.length == replicaCount);

    long blockGroupLen = Long.MAX_VALUE;

    for (int i = 0; i < replicaCount; i++) {
      if (blockGroup[i] == null) {
        continue;
      }

      String putBlockLenStr = blockGroup[i].getMetadata()
          .get(OzoneConsts.BLOCK_GROUP_LEN_KEY_IN_PUT_BLOCK);
      long putBlockLen = (putBlockLenStr == null) ?
          Long.MAX_VALUE :
          Long.parseLong(putBlockLenStr);
      // Use the min to be conservative
      blockGroupLen = Math.min(putBlockLen, blockGroupLen);
    }
    return blockGroupLen == Long.MAX_VALUE ? 0 : blockGroupLen;
  }
}