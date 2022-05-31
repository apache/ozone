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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactory;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactoryImpl;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
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
    return null;
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
}
