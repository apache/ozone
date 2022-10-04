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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.hdds.scm.ContainerClientMetrics;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.scm.storage.BufferPool;
import org.apache.hadoop.hdds.scm.storage.ECBlockOutputStream;
import org.apache.hadoop.hdds.security.token.ContainerTokenIdentifier;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactory;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactoryImpl;
import org.apache.hadoop.ozone.client.io.ECBlockInputStreamProxy;
import org.apache.hadoop.ozone.client.io.ECBlockReconstructedStripeInputStream;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.security.token.Token;
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
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.container.ec.reconstruction.TokenHelper.encode;

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

  private final ECContainerOperationClient containerOperationClient;

  private final ByteBufferPool byteBufferPool;
  private final CertificateClient certificateClient;

  private final ExecutorService ecReconstructExecutor;

  private final BlockInputStreamFactory blockInputStreamFactory;
  private final TokenHelper tokenHelper;
  private final ContainerClientMetrics clientMetrics;

  public ECReconstructionCoordinator(ConfigurationSource conf,
                                     CertificateClient certificateClient)
      throws IOException {
    this.containerOperationClient = new ECContainerOperationClient(conf,
        certificateClient);
    this.byteBufferPool = new ElasticByteBufferPool();
    this.certificateClient = certificateClient;
    this.ecReconstructExecutor =
        new ThreadPoolExecutor(EC_RECONSTRUCT_STRIPE_READ_POOL_MIN_SIZE,
            conf.getObject(OzoneClientConfig.class)
                .getEcReconstructStripeReadPoolLimit(), 60, TimeUnit.SECONDS,
            new SynchronousQueue<>(), new ThreadFactoryBuilder()
            .setNameFormat("ec-reconstruct-reader-TID-%d").build(),
            new ThreadPoolExecutor.CallerRunsPolicy());
    this.blockInputStreamFactory = BlockInputStreamFactoryImpl
        .getInstance(byteBufferPool, () -> ecReconstructExecutor);
    tokenHelper = new TokenHelper(conf, certificateClient);
    this.clientMetrics = ContainerClientMetrics.acquire();
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
        Integer index = indexDnPair.getKey();
        containerOperationClient
            .createRecoveringContainer(containerID, dn, repConfig,
                containerToken, index);
        recoveringContainersCreatedDNs.add(dn);
      }

      // 2. Reconstruct and transfer to targets
      for (BlockLocationInfo blockLocationInfo : blockLocationInfoMap
          .values()) {
        reconstructECBlockGroup(blockLocationInfo, repConfig, targetNodeMap);
      }

      // 3. Close containers
      for (DatanodeDetails dn: recoveringContainersCreatedDNs) {
        containerOperationClient
            .closeContainer(containerID, dn, repConfig, containerToken);
      }
    } catch (Exception e) {
      // Any exception let's delete the recovering containers.
      LOG.warn(
          "Exception while reconstructing the container {}. Cleaning up"
              + " all the recovering containers in the reconstruction process.",
          containerID, e);
      // Delete only the current thread successfully created recovering
      // containers.
      for (DatanodeDetails dn : recoveringContainersCreatedDNs) {
        try {
          containerOperationClient
              .deleteRecoveringContainer(containerID, dn, repConfig,
                  containerToken);
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

  void reconstructECBlockGroup(BlockLocationInfo blockLocationInfo,
      ECReplicationConfig repConfig,
      SortedMap<Integer, DatanodeDetails> targetMap)
      throws IOException {
    long safeBlockGroupLength = blockLocationInfo.getLength();
    List<Integer> missingContainerIndexes = new ArrayList<>(targetMap.keySet());

    // calculate the real missing block indexes
    int dataLocs = ECBlockInputStreamProxy
        .expectedDataLocations(repConfig, safeBlockGroupLength);
    List<Integer> toReconstructIndexes = new ArrayList<>();
    for (Integer index : missingContainerIndexes) {
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
      OzoneClientConfig configuration = new OzoneClientConfig();
      // TODO: Let's avoid unnecessary bufferPool creation. This pool actually
      //  not used in EC flows, but there are some dependencies on buffer pool.
      BufferPool bufferPool =
          new BufferPool(configuration.getStreamBufferSize(),
              (int) (configuration.getStreamBufferMaxSize() / configuration
                  .getStreamBufferSize()),
              ByteStringConversion.createByteBufferConversion(false));
      for (int i = 0; i < toReconstructIndexes.size(); i++) {
        DatanodeDetails datanodeDetails =
            targetMap.get(toReconstructIndexes.get(i));
        targetBlockStreams[i] =
            new ECBlockOutputStream(blockLocationInfo.getBlockID(),
                this.containerOperationClient.getXceiverClientManager(),
                this.containerOperationClient
                    .singleNodePipeline(datanodeDetails, repConfig), bufferPool,
                configuration, blockLocationInfo.getToken(), clientMetrics);
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
          CompletableFuture<ContainerProtos.ContainerCommandResponseProto>
              future = targetBlockStreams[i].write(bufs[i]);
          checkFailures(targetBlockStreams[i], future);
          bufs[i].clear();
        }
        length -= readLen;
      }

      try {
        for (ECBlockOutputStream targetStream : targetBlockStreams) {
          targetStream
              .executePutBlock(true, true, blockLocationInfo.getLength());
          checkFailures(targetStream,
              targetStream.getCurrentPutBlkResponseFuture());
        }
      } finally {
        for (ByteBuffer buf : bufs) {
          byteBufferPool.putBuffer(buf);
        }
        IOUtils.cleanupWithLogger(LOG, targetBlockStreams);
      }
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
      throw new IOException(
          "Chunk write failed at the new target node: " + targetBlockStream
              .getDatanodeDetails() + ". Aborting the reconstruction process.");
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
    tokenHelper.stop();
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
        tokenHelper.getContainerToken(new ContainerID(containerID));

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