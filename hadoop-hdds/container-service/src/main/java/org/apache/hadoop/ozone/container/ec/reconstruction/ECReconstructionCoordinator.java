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
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactory;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactoryImpl;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The Coordinator implements the main flow of reconstructing
 * missing container replicas.
 *
 * For a container reconstruction task, the main flow is:
 *   - ListBlock from all healthy replicas
 *   - calculate effective block group len for all blocks
 *   - for each block
 *       - build a ReconstructInputStream to read healthy chunks
 *       - build a ECBlockOutputStream to write out decoded chunks
 *       - for each stripe
 *         - use ReconstructInputStream.readStripe to decode missing chunks
 *         - use ECBlockOutputStream.write to write decoded chunks to TargetDNs
 *       - PutBlock
 *   - CloseContainer
 */
public class ECReconstructionCoordinator implements Closeable {

  private static final int EC_RECONSTRUCT_STRIPE_READ_POOL_MIN_SIZE = 3;

  private final ECContainerOperationClient containerOperationClient;

  private final ConfigurationSource config;

  private final DatanodeDetails localDatanode;

  private final ByteBufferPool byteBufferPool;

  private ExecutorService ecReconstructExecutor;

  private BlockInputStreamFactory blockInputStreamFactory;

  public ECReconstructionCoordinator(
      ECContainerOperationClient containerClient,
      ConfigurationSource conf, DatanodeDetails localDN,
      ByteBufferPool byteBufferPool,
      ExecutorService reconstructExecutor,
      BlockInputStreamFactory streamFactory) {
    this.containerOperationClient = containerClient;
    this.config = conf;
    this.localDatanode = localDN;
    this.byteBufferPool = byteBufferPool;
    this.blockInputStreamFactory = streamFactory;
    this.ecReconstructExecutor = reconstructExecutor;
  }

  public ECReconstructionCoordinator(ConfigurationSource conf,
      DatanodeDetails localDN) throws IOException {
    this(new ECContainerOperationClient(conf), conf, localDN,
        new ElasticByteBufferPool(), null, null);
    this.ecReconstructExecutor = new ThreadPoolExecutor(
        EC_RECONSTRUCT_STRIPE_READ_POOL_MIN_SIZE,
        config.getObject(OzoneClientConfig.class)
            .getEcReconstructStripeReadPoolLimit(),
        60, TimeUnit.SECONDS, new SynchronousQueue<>(),
        new ThreadFactoryBuilder()
            .setNameFormat("ec-reconstruct-reader-TID-%d")
            .build(),
        new ThreadPoolExecutor.CallerRunsPolicy());
    this.blockInputStreamFactory = BlockInputStreamFactoryImpl
        .getInstance(byteBufferPool, () -> ecReconstructExecutor);
  }

  public void reconstructECContainerGroup(long containerID,
      ECReplicationConfig repConfig,
      SortedMap<Integer, DatanodeDetails> sourceNodeMap,
      SortedMap<Integer, DatanodeDetails> targetNodeMap) {

    Pipeline pipeline = rebuildPipeline(repConfig,
        sourceNodeMap, targetNodeMap);

    SortedMap<Long, BlockData[]> blockDataMap = getBlockDataMap(
        containerID, repConfig, sourceNodeMap);

    SortedMap<Long, BlockLocationInfo> blockLocationInfoMap =
        calcBlockLocationInfoMap(containerID, blockDataMap, pipeline);

    for (BlockLocationInfo blockLocationInfo : blockLocationInfoMap.values()) {
      reconstructECBlockGroup(blockLocationInfo, repConfig);
    }
  }

  void reconstructECBlockGroup(BlockLocationInfo blockLocationInfo,
      ECReplicationConfig repConfig) {

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
            .setLength(blockGroupLen)
            .setPipeline(pipeline)
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
  }

  private Pipeline rebuildPipeline(ECReplicationConfig repConfig,
      SortedMap<Integer, DatanodeDetails> sourceNodeMap,
      SortedMap<Integer, DatanodeDetails> targetNodeMap) {

    List<DatanodeDetails> nodes = new ArrayList<>(sourceNodeMap.values());
    nodes.addAll(targetNodeMap.values());

    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(repConfig)
        .setNodes(nodes)
        .setState(Pipeline.PipelineState.CLOSED)
        .build();
  }

  private SortedMap<Long, BlockData[]> getBlockDataMap(long containerID,
      ECReplicationConfig repConfig,
      Map<Integer, DatanodeDetails> sourceNodeMap) {
    return null;
  }

  /**
   * Get the effective length of each block group.
   * We can not be absolutely accurate when there is a failed stripe
   * in this block since the failed cells could be missing, and
   * we can not tell from the healthy cells whether the last stripe
   * is failed or not. But in such case we at most recover one extra
   * stripe for this block which does not confuse the client data view.
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
      long putBlockLen = (putBlockLenStr == null)
          ? Long.MAX_VALUE : Long.parseLong(putBlockLenStr);
      // Use the min to be conservative
      blockGroupLen = Math.min(putBlockLen, blockGroupLen);
    }
    return blockGroupLen == Long.MAX_VALUE ? 0 : blockGroupLen;
  }
}
