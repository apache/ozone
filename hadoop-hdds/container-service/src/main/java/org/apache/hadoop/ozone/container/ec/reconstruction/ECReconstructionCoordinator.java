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

package org.apache.hadoop.ozone.container.ec.reconstruction;

import static org.apache.hadoop.ozone.container.common.helpers.TokenHelper.encode;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
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
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.StreamBufferArgs;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.hdds.scm.storage.ECBlockOutputStream;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.SecretKeySignerClient;
import org.apache.hadoop.hdds.security.token.ContainerTokenIdentifier;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactory;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactoryImpl;
import org.apache.hadoop.ozone.client.io.ECBlockInputStreamProxy;
import org.apache.hadoop.ozone.client.io.ECBlockReconstructedStripeInputStream;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.TokenHelper;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.security.token.Token;
import org.apache.ratis.util.MemoizedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Coordinator implements the main flow of reconstructing
 * missing container replicas.
 * <p>
 * For a container reconstruction task, the main flow is:
 * - ListBlock from all healthy replicas
 * - calculate effective block group len for all blocks
 * - create RECOVERING containers in TargetDNs
 * -  for each block
 * -    build a ECReconstructedStripedInputStream to read healthy chunks
 * -    build a ECBlockOutputStream to write out decoded chunks
 * -      for each stripe
 * -        use ECReconstructedStripedInputStream.recoverChunks to decode chunks
 * -        use ECBlockOutputStream.write to write decoded chunks to TargetDNs
 * -    PutBlock
 * - Close RECOVERING containers in TargetDNs
 */
public class ECReconstructionCoordinator implements Closeable {

  static final Logger LOG =
      LoggerFactory.getLogger(ECReconstructionCoordinator.class);

  private static final int EC_RECONSTRUCT_STRIPE_READ_POOL_MIN_SIZE = 3;

  private static final int EC_RECONSTRUCT_STRIPE_WRITE_POOL_MIN_SIZE = 5;

  private final ECContainerOperationClient containerOperationClient;

  private final ByteBufferPool byteBufferPool;

  private final ExecutorService ecReconstructReadExecutor;
  private final MemoizedSupplier<ExecutorService> ecReconstructWriteExecutor;
  private final BlockInputStreamFactory blockInputStreamFactory;
  private final TokenHelper tokenHelper;
  private final ContainerClientMetrics clientMetrics;
  private final ECReconstructionMetrics metrics;
  private final StateContext context;
  private final OzoneClientConfig ozoneClientConfig;

  public ECReconstructionCoordinator(
      ConfigurationSource conf, CertificateClient certificateClient,
      SecretKeySignerClient secretKeyClient, StateContext context,
      ECReconstructionMetrics metrics,
      String threadNamePrefix) throws IOException {
    this.context = context;
    this.containerOperationClient = new ECContainerOperationClient(conf,
        certificateClient);
    this.byteBufferPool = new ElasticByteBufferPool();
    ozoneClientConfig = conf.getObject(OzoneClientConfig.class);
    this.ecReconstructReadExecutor = createThreadPoolExecutor(
        EC_RECONSTRUCT_STRIPE_READ_POOL_MIN_SIZE,
        ozoneClientConfig.getEcReconstructStripeReadPoolLimit(),
        threadNamePrefix + "ec-reconstruct-reader-TID-%d");
    this.ecReconstructWriteExecutor = MemoizedSupplier.valueOf(
        () -> createThreadPoolExecutor(
            EC_RECONSTRUCT_STRIPE_WRITE_POOL_MIN_SIZE,
            ozoneClientConfig.getEcReconstructStripeWritePoolLimit(),
            threadNamePrefix + "ec-reconstruct-writer-TID-%d"));
    this.blockInputStreamFactory = BlockInputStreamFactoryImpl
        .getInstance(byteBufferPool, () -> ecReconstructReadExecutor);
    tokenHelper = new TokenHelper(new SecurityConfig(conf), secretKeyClient);
    this.clientMetrics = ContainerClientMetrics.acquire();
    this.metrics = metrics;
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
    ContainerID cid = ContainerID.valueOf(containerID);

    // 1. create target recovering containers.
    String containerToken = encode(tokenHelper.getContainerToken(cid));
    List<DatanodeDetails> recoveringContainersCreatedDNs = new ArrayList<>();
    try {
      for (Map.Entry<Integer, DatanodeDetails> indexDnPair : targetNodeMap
          .entrySet()) {
        DatanodeDetails dn = indexDnPair.getValue();
        int index = indexDnPair.getKey();
        LOG.debug("Creating container {} on datanode {} for index {}",
            containerID, dn, index);
        containerOperationClient
            .createRecoveringContainer(containerID, dn, repConfig,
                containerToken, index);
        recoveringContainersCreatedDNs.add(dn);
      }

      // 2. Reconstruct and transfer to targets
      for (Map.Entry<Long, BlockLocationInfo> blockLocationInfoEntry
          : blockLocationInfoMap.entrySet()) {
        Long key = blockLocationInfoEntry.getKey();
        BlockLocationInfo blockLocationInfo = blockLocationInfoEntry.getValue();
        reconstructECBlockGroup(blockLocationInfo, repConfig,
            targetNodeMap, blockDataMap.get(key));
      }

      // 3. Close containers
      for (DatanodeDetails dn: recoveringContainersCreatedDNs) {
        LOG.debug("Closing container {} on datanode {}", containerID, dn);
        containerOperationClient
            .closeContainer(containerID, dn, repConfig, containerToken);
      }
      metrics.incReconstructionTotal();
      metrics.incBlockGroupReconstructionTotal(blockLocationInfoMap.size());
    } catch (Exception e) {
      // Any exception let's delete the recovering containers.
      metrics.incReconstructionFailsTotal();
      metrics.incBlockGroupReconstructionFailsTotal(
          blockLocationInfoMap.size());
      LOG.warn(
          "Exception while reconstructing the container {}. Cleaning up"
              + " all the recovering containers in the reconstruction process.",
          containerID, e);
      // Delete only the current thread successfully created recovering
      // containers.
      for (DatanodeDetails dn : recoveringContainersCreatedDNs) {
        try {
          containerOperationClient
              .deleteContainerInState(containerID, dn, repConfig,
                  containerToken, ImmutableSet.of(
                          ContainerProtos.ContainerDataProto.State.UNHEALTHY,
                          ContainerProtos.ContainerDataProto.State.RECOVERING));
          if (LOG.isDebugEnabled()) {
            LOG.debug("Deleted the container {}, at the target: {}",
                containerID, dn);
          }
        } catch (IOException ioe) {
          LOG.error("Exception while deleting the container {} at target: {}",
              containerID, dn, ioe);
        }
      }
      throw e;
    }

  }

  private ECBlockOutputStream getECBlockOutputStream(
      BlockLocationInfo blockLocationInfo, DatanodeDetails datanodeDetails,
      ECReplicationConfig repConfig, int replicaIndex) throws IOException {
    StreamBufferArgs streamBufferArgs =
        StreamBufferArgs.getDefaultStreamBufferArgs(repConfig, ozoneClientConfig);
    return new ECBlockOutputStream(
        blockLocationInfo.getBlockID(),
        containerOperationClient.getXceiverClientManager(),
        containerOperationClient.singleNodePipeline(datanodeDetails,
            repConfig, replicaIndex),
        BufferPool.empty(), ozoneClientConfig,
        blockLocationInfo.getToken(), clientMetrics, streamBufferArgs, ecReconstructWriteExecutor);
  }

  @VisibleForTesting
  public void reconstructECBlockGroup(BlockLocationInfo blockLocationInfo,
      ECReplicationConfig repConfig,
      SortedMap<Integer, DatanodeDetails> targetMap, BlockData[] blockDataGroup)
      throws IOException {
    long safeBlockGroupLength = blockLocationInfo.getLength();
    List<Integer> missingContainerIndexes = new ArrayList<>(targetMap.keySet());

    // calculate the real missing block indexes
    int dataLocs = ECBlockInputStreamProxy
        .expectedDataLocations(repConfig, safeBlockGroupLength);
    List<Integer> toReconstructIndexes = new ArrayList<>();
    List<Integer> notReconstructIndexes = new ArrayList<>();
    for (Integer index : missingContainerIndexes) {
      if (index <= dataLocs || index > repConfig.getData()) {
        toReconstructIndexes.add(index);
      } else {
        // Don't need to be reconstructed, but we do need a stream to write
        // the block data to.
        notReconstructIndexes.add(index);
      }
    }

    OzoneClientConfig clientConfig = this.ozoneClientConfig;
    clientConfig.setChecksumVerify(true);
    try (ECBlockReconstructedStripeInputStream sis
        = new ECBlockReconstructedStripeInputStream(
        repConfig, blockLocationInfo,
        this.containerOperationClient.getXceiverClientManager(), null,
        this.blockInputStreamFactory, byteBufferPool,
        this.ecReconstructReadExecutor,
        clientConfig)) {

      ECBlockOutputStream[] targetBlockStreams =
          new ECBlockOutputStream[toReconstructIndexes.size()];
      ECBlockOutputStream[] emptyBlockStreams =
          new ECBlockOutputStream[notReconstructIndexes.size()];
      ByteBuffer[] bufs = new ByteBuffer[toReconstructIndexes.size()];
      try {
        // Create streams and buffers for all indexes that need reconstructed
        for (int i = 0; i < toReconstructIndexes.size(); i++) {
          int replicaIndex = toReconstructIndexes.get(i);
          DatanodeDetails datanodeDetails = targetMap.get(replicaIndex);
          targetBlockStreams[i] = getECBlockOutputStream(blockLocationInfo, datanodeDetails, repConfig, replicaIndex);
          bufs[i] = byteBufferPool.getBuffer(false, repConfig.getEcChunkSize());
          bufs[i].clear();
        }
        // Then create a stream for all indexes that don't need reconstructed, but still need a stream to
        // write the empty block data to.
        for (int i = 0; i < notReconstructIndexes.size(); i++) {
          int replicaIndex = notReconstructIndexes.get(i);
          DatanodeDetails datanodeDetails = targetMap.get(replicaIndex);
          emptyBlockStreams[i] = getECBlockOutputStream(blockLocationInfo, datanodeDetails, repConfig, replicaIndex);
        }

        if (!toReconstructIndexes.isEmpty()) {
          sis.setRecoveryIndexes(toReconstructIndexes.stream().map(i -> (i - 1))
              .collect(Collectors.toSet()));
          long length = safeBlockGroupLength;
          while (length > 0) {
            int readLen;
            try {
              readLen = sis.recoverChunks(bufs);
              Set<Integer> failedIndexes = sis.getFailedIndexes();
              if (!failedIndexes.isEmpty()) {
                // There was a problem reading some of the block indexes, but we
                // did not get an exception as there must have been spare indexes
                // to try and recover from. Therefore we should log out the block
                // group details in the same way as for the exception case below.
                logBlockGroupDetails(blockLocationInfo, repConfig,
                    blockDataGroup);
              }
            } catch (IOException e) {
              // When we see exceptions here, it could be due to some transient
              // issue that causes the block read to fail when reconstructing it,
              // but we have seen issues where the containers don't have the
              // blocks they appear they should have, or the block chunks are the
              // wrong length etc. In order to debug these sort of cases, if we
              // get an error, we will log out the details about the block group
              // length on each source, along with their chunk list and chunk
              // lengths etc.
              logBlockGroupDetails(blockLocationInfo, repConfig,
                  blockDataGroup);
              throw e;
            }
            // TODO: can be submitted in parallel
            for (int i = 0; i < bufs.length; i++) {
              if (bufs[i].remaining() != 0) {
                // If the buffer is empty, we don't need to write it as it will cause
                // an empty chunk to be added to the end of the block.
                CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
                    future = targetBlockStreams[i].write(bufs[i]);
                checkFailures(targetBlockStreams[i], future);
              }
              bufs[i].clear();
            }
            length -= readLen;
          }
        }
        List<ECBlockOutputStream> allStreams = new ArrayList<>(Arrays.asList(targetBlockStreams));
        allStreams.addAll(Arrays.asList(emptyBlockStreams));
        for (ECBlockOutputStream targetStream : allStreams) {
          targetStream.executePutBlock(true, true, blockLocationInfo.getLength(), blockDataGroup);
          checkFailures(targetStream, targetStream.getCurrentPutBlkResponseFuture());
        }
      } finally {
        for (ByteBuffer buf : bufs) {
          byteBufferPool.putBuffer(buf);
        }
        IOUtils.cleanupWithLogger(LOG, targetBlockStreams);
        IOUtils.cleanupWithLogger(LOG, emptyBlockStreams);
      }
    }
  }

  private void logBlockGroupDetails(BlockLocationInfo blockLocationInfo,
      ECReplicationConfig repConfig, BlockData[] blockDataGroup) {
    LOG.info("Block group details for {}. " +
        "Replication Config {}. Calculated safe length: {}. ",
        blockLocationInfo.getBlockID(), repConfig,
        blockLocationInfo.getLength());
    for (int i = 0; i < blockDataGroup.length; i++) {
      BlockData data = blockDataGroup[i];
      if (data == null) {
        continue;
      }
      StringBuilder sb = new StringBuilder();
      sb.append("Block Data for: ")
          .append(data.getBlockID())
          .append(" replica Index: ")
          .append(i + 1)
          .append(" block length: ")
          .append(data.getSize())
          .append(" block group length: ")
          .append(data.getBlockGroupLength())
          .append(" chunk list: \n");
      int cnt = 0;
      for (ContainerProtos.ChunkInfo chunkInfo : data.getChunks()) {
        if (cnt > 0) {
          sb.append('\n');
        }
        sb.append("  chunkNum: ")
            .append(++cnt)
            .append(" length: ")
            .append(chunkInfo.getLen())
            .append(" offset: ")
            .append(chunkInfo.getOffset());
      }
      LOG.info(sb.toString());
    }
  }

  private void checkFailures(ECBlockOutputStream targetBlockStream,
      CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
          currentPutBlkResponseFuture)
      throws IOException {
    if (isFailed(targetBlockStream, currentPutBlkResponseFuture)) {
      // If one chunk response failed, we should retry.
      // Even after retries if it failed, we should declare the
      // reconstruction as failed.
      // For now, let's throw the exception.
      throw new IOException("Chunk write failed at the new target node: " +
          targetBlockStream.getDatanodeDetails() +
          ". Aborting the reconstruction process.",
          targetBlockStream.getIoException());
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
        BlockID blockID = new BlockID(containerID, localID);
        BlockLocationInfo blockLocationInfo = new BlockLocationInfo.Builder()
            .setBlockID(blockID)
            .setLength(blockGroupLen)
            .setPipeline(pipeline)
            .setToken(tokenHelper.getBlockToken(blockID, blockGroupLen))
            .build();
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
    if (ecReconstructWriteExecutor.isInitialized()) {
      ecReconstructWriteExecutor.get().shutdownNow();
    }
    ecReconstructReadExecutor.shutdownNow();
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
    Token<ContainerTokenIdentifier> containerToken =
        tokenHelper.getContainerToken(ContainerID.valueOf(containerID));

    Iterator<Map.Entry<Integer, DatanodeDetails>> iterator =
        sourceNodeMap.entrySet().iterator();

    while (iterator.hasNext()) {
      Map.Entry<Integer, DatanodeDetails> next = iterator.next();
      Integer index = next.getKey();
      DatanodeDetails dn = next.getValue();

      BlockData[] blockDataArr = containerOperationClient.listBlock(
          containerID, dn, repConfig, containerToken);

      for (BlockData blockData : blockDataArr) {
        BlockID blockID = blockData.getBlockID();
        BlockData[] blkDataArr = resultMap.getOrDefault(blockData.getLocalID(),
            new BlockData[repConfig.getRequiredNodes()]);
        blkDataArr[index - 1] = blockData;
        resultMap.put(blockID.getLocalID(), blkDataArr);
      }
    }
    // When a stripe is written, the put block is sent to all nodes even if
    // that nodes has zero bytes written to it. If the
    // client does not get an ACK from all nodes, it will abandon the stripe,
    // which can leave incomplete stripes on the DNs. Therefore, we should check
    // that all blocks in the result map have an entry for all nodes. If they
    // do not, it means this is an abandoned stripe and we should not attempt
    // to reconstruct it.
    // Note that if some nodes report different values for the block length,
    // it also indicate garbage data at the end of the block. A different part
    // of the code handles this and only reconstructs the valid part of the
    // block, ie the minimum length reported by the nodes.
    Iterator<Map.Entry<Long, BlockData[]>> resultIterator
        = resultMap.entrySet().iterator();
    while (resultIterator.hasNext()) {
      Map.Entry<Long, BlockData[]> entry = resultIterator.next();
      BlockData[] blockDataArr = entry.getValue();
      for (Map.Entry<Integer, DatanodeDetails> e : sourceNodeMap.entrySet()) {
        // There should be an entry in the Array for each keyset node. If there
        // is not, this is an orphaned stripe and we should remove it from the
        // result.
        if (blockDataArr[e.getKey() - 1] == null) {
          LOG.warn("In container {} block {} does not have a putBlock entry " +
              "for index {} on datanode {} making it an orphan block / " +
              "stripe. It will not be reconstructed", containerID,
              entry.getKey(), e.getKey(), e.getValue());
          resultIterator.remove();
          break;
        }
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

      long putBlockLen = blockGroup[i].getBlockGroupLength();
      // Use safe length is the minimum of the lengths recorded across the
      // stripe
      blockGroupLen = Math.min(putBlockLen, blockGroupLen);
    }
    return blockGroupLen == Long.MAX_VALUE ? 0 : blockGroupLen;
  }

  public ECReconstructionMetrics getECReconstructionMetrics() {
    return this.metrics;
  }

  OptionalLong getTermOfLeaderSCM() {
    return Optional.ofNullable(context)
        .map(StateContext::getTermOfLeaderSCM)
        .orElse(OptionalLong.empty());
  }

  private static ExecutorService createThreadPoolExecutor(
      int corePoolSize, int maximumPoolSize, String threadNameFormat) {
    return new ThreadPoolExecutor(corePoolSize, maximumPoolSize,
        60, TimeUnit.SECONDS, new SynchronousQueue<>(),
        new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build(),
        new ThreadPoolExecutor.CallerRunsPolicy());
  }
}
