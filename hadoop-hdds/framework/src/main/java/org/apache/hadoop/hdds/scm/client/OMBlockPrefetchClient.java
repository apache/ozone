/**
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
package org.apache.hadoop.hdds.scm.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.net.NodeImpl;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.apache.hadoop.hdds.scm.net.NetConstants.NODE_COST_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MAX_BLOCKS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MAX_BLOCKS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCHED_BLOCKS_EXPIRY_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCHED_BLOCKS_EXPIRY_INTERVAL_DEFAULT;

/**
 * OMBlockPrefetchClient manages block prefetching for efficient write operations.
 * It maintains a queue of allocated blocks per replication configuration and periodically removes expired blocks.
 * The client refills the queue in the background to handle high-throughput scenarios efficiently.
 */
public class OMBlockPrefetchClient {
  private static final Logger LOG = LoggerFactory.getLogger(OMBlockPrefetchClient.class);
  private final ScmBlockLocationProtocol scmBlockLocationProtocol;
  private int maxBlocks;
  private boolean useHostname;
  private DNSToSwitchMapping dnsToSwitchMapping;
  private ScheduledExecutorService prefetchExecutor;
  private final Map<ReplicationConfig, ConcurrentLinkedDeque<ExpiringAllocatedBlock>> blockQueueMap =
      new ConcurrentHashMap<>();
  private long expiryDuration;

  // TODO: test fields, make configurable later
  private static final int ADDITIONAL_BLOCKS_MAX = 20;
  private static final int MAX_PARALLEL_CACHE_BUILDUP = 500;
  private static final AtomicInteger PARALLEL_ADDITIONAL_BLOCKS = new AtomicInteger(0);

  private OMBlockPrefetchMetrics metrics;

  private static final ReplicationConfig RATIS_THREE =
      ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);
  private static final ReplicationConfig RATIS_ONE =
      ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.ONE);
  private static final ReplicationConfig RS_3_2_1024 =
      ReplicationConfig.fromProto(HddsProtos.ReplicationType.EC, null,
          toProto(3, 2, ECReplicationConfig.EcCodec.RS, 1024));
  private static final ReplicationConfig RS_6_3_1024 =
      ReplicationConfig.fromProto(HddsProtos.ReplicationType.EC, null,
          toProto(6, 3, ECReplicationConfig.EcCodec.RS, 1024));
  private static final ReplicationConfig XOR_10_4_4096 =
      ReplicationConfig.fromProto(HddsProtos.ReplicationType.EC, null,
          toProto(10, 4, ECReplicationConfig.EcCodec.XOR, 4096));

  public static HddsProtos.ECReplicationConfig toProto(int data, int parity, ECReplicationConfig.EcCodec codec,
                                                       int ecChunkSize) {
    return HddsProtos.ECReplicationConfig.newBuilder()
        .setData(data)
        .setParity(parity)
        .setCodec(codec.toString())
        .setEcChunkSize(ecChunkSize)
        .build();
  }

  public OMBlockPrefetchClient(ScmBlockLocationProtocol scmBlockClient) {
    this.scmBlockLocationProtocol = scmBlockClient;
    initializeBlockQueueMap();
  }

  private void initializeBlockQueueMap() {
    blockQueueMap.put(RATIS_THREE, new ConcurrentLinkedDeque<>());
    blockQueueMap.put(RATIS_ONE, new ConcurrentLinkedDeque<>());
    blockQueueMap.put(RS_3_2_1024, new ConcurrentLinkedDeque<>());
    blockQueueMap.put(RS_6_3_1024, new ConcurrentLinkedDeque<>());
    blockQueueMap.put(XOR_10_4_4096, new ConcurrentLinkedDeque<>());
  }

  private static final class ExpiringAllocatedBlock {
    private final AllocatedBlock block;
    private final long expiryTime;

    private ExpiringAllocatedBlock(AllocatedBlock block, long expiryTime) {
      this.block = block;
      this.expiryTime = expiryTime;
    }

    private AllocatedBlock getBlock() {
      return block;
    }

    private long getExpiryTime() {
      return expiryTime;
    }
  }

  public void start(ConfigurationSource conf) throws IOException, InterruptedException, TimeoutException {
    maxBlocks = conf.getInt(OZONE_OM_PREFETCH_MAX_BLOCKS, OZONE_OM_PREFETCH_MAX_BLOCKS_DEFAULT);
    expiryDuration = conf.getTimeDuration(OZONE_OM_PREFETCHED_BLOCKS_EXPIRY_INTERVAL,
        OZONE_OM_PREFETCHED_BLOCKS_EXPIRY_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);

    useHostname = conf.getBoolean(DFSConfigKeysLegacy.DFS_DATANODE_USE_DN_HOSTNAME,
        DFSConfigKeysLegacy.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    Class<? extends DNSToSwitchMapping> dnsToSwitchMappingClass =
        conf.getClass(DFSConfigKeysLegacy.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            TableMapping.class, DNSToSwitchMapping.class);
    DNSToSwitchMapping newInstance = ReflectionUtils.newInstance(
        dnsToSwitchMappingClass, OzoneConfiguration.of(conf));
    dnsToSwitchMapping =
        ((newInstance instanceof CachedDNSToSwitchMapping) ? newInstance
            : new CachedDNSToSwitchMapping(newInstance));

    prefetchExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("BlockPrefetchThread-%d").setDaemon(true).build()
    );
    metrics = OMBlockPrefetchMetrics.register();
  }

  public void stop() {
    if (prefetchExecutor != null) {
      prefetchExecutor.shutdown();
      try {
        if (!prefetchExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          prefetchExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted while shutting down prefetch executor service.", e);
        Thread.currentThread().interrupt();
      }
    }
    OMBlockPrefetchMetrics.unregister();
  }

  public int blocksToFetch(int queueSize, int numBlocks) {
    // Ensure we do not exceed configured max queue size
    int availableSpace = Math.max(0, maxBlocks - queueSize);

    // Ensure we do not exceed the max parallel cache buildup
    int parallelBlocks = PARALLEL_ADDITIONAL_BLOCKS.get();
    int allowedPrefetch = Math.max(0, MAX_PARALLEL_CACHE_BUILDUP - parallelBlocks);
    int finalBlocksToFetch = Math.min(Math.min(numBlocks, availableSpace), allowedPrefetch);

    if (finalBlocksToFetch > 0) {
      PARALLEL_ADDITIONAL_BLOCKS.addAndGet(finalBlocksToFetch);
    }

    return finalBlocksToFetch;
  }

  @SuppressWarnings("parameternumber")
  public List<AllocatedBlock> getBlocks(long scmBlockSize, int numBlocks, ReplicationConfig replicationConfig,
                                        String serviceID, ExcludeList excludeList, String clientMachine,
                                        NetworkTopology clusterMap) throws IOException {
    long readStartTime = Time.monotonicNowNanos();
    List<AllocatedBlock> allocatedBlocks = new ArrayList<>();
    int retrievedBlocksCount = 0;
    ConcurrentLinkedDeque<ExpiringAllocatedBlock> queue = blockQueueMap.get(replicationConfig);

    // We redirect to the allocateBlock RPC call to SCM when we encounter an untested ReplicationConfig or a populated
    // ExcludeList, otherwise we return blocks from cache.
    if (queue != null && excludeList.isEmpty()) {
      while (retrievedBlocksCount < numBlocks) {
        ExpiringAllocatedBlock expiringBlock = queue.poll();
        if (expiringBlock == null) {
          break;
        }

        if (System.currentTimeMillis() > expiringBlock.getExpiryTime()) {
          continue;
        }

        AllocatedBlock block = expiringBlock.getBlock();
        List<DatanodeDetails> sortedNodes = sortDatanodes(block.getPipeline().getNodes(), clientMachine, clusterMap);
        if (!Objects.equals(sortedNodes, block.getPipeline().getNodesInOrder())) {
          block = block.toBuilder()
              .setPipeline(block.getPipeline().copyWithNodesInOrder(sortedNodes))
              .build();
        }
        allocatedBlocks.add(block);
        retrievedBlocksCount++;
      }

      int remainingBlocks = numBlocks - retrievedBlocksCount;

      // If there aren't enough blocks in the cache, we make an RPC call to SCM and prefetch additional batch max + the
      // requested blocks in the same call, avoiding a separate prefetchBlocks call to SCM.
      int overAllocateBlocks = (remainingBlocks > 0) ? blocksToFetch(queue.size(), ADDITIONAL_BLOCKS_MAX) : 0;
      List<AllocatedBlock> newBlocks =
          scmBlockLocationProtocol.allocateBlock(scmBlockSize, remainingBlocks + overAllocateBlocks, replicationConfig,
              serviceID, excludeList, clientMachine);
      long expiryTime = System.currentTimeMillis() + expiryDuration;
      int newBlocksSize = newBlocks.size();
      allocatedBlocks.addAll(newBlocks.subList(0, remainingBlocks));

      if (overAllocateBlocks > 0) {
        for (AllocatedBlock block : newBlocks.subList(remainingBlocks, newBlocksSize)) {
          queue.offer(new ExpiringAllocatedBlock(block, expiryTime));
        }
        LOG.info("Prefetched {} blocks for replication config {}.", newBlocksSize, replicationConfig);
        PARALLEL_ADDITIONAL_BLOCKS.addAndGet(-overAllocateBlocks);
      }

      if (remainingBlocks > 0) {
        metrics.incrementCacheMisses();
      } else {
        metrics.incrementCacheHits();
      }

      metrics.addReadFromQueueLatency(Time.monotonicNowNanos() - readStartTime);
      return allocatedBlocks;

    } else {
      metrics.addReadFromQueueLatency(Time.monotonicNowNanos() - readStartTime);
      metrics.incrementCacheMisses();
      return scmBlockLocationProtocol.allocateBlock(scmBlockSize, numBlocks, replicationConfig, serviceID, excludeList,
          clientMachine);
    }
  }

  public List<DatanodeDetails> sortDatanodes(List<DatanodeDetails> nodes, String clientMachine,
                                             NetworkTopology clusterMap) {
    long sortStartTime = Time.monotonicNowNanos();
    final Node client = getClientNode(clientMachine, nodes, clusterMap);
    List<DatanodeDetails> sortedNodes = clusterMap.sortByDistanceCost(client, nodes, nodes.size());
    metrics.addSortingLogicLatency(Time.monotonicNowNanos() - sortStartTime);
    return sortedNodes;
  }

  private Node getClientNode(String clientMachine, List<DatanodeDetails> nodes, NetworkTopology clusterMap) {
    List<DatanodeDetails> matchingNodes = new ArrayList<>();
    for (DatanodeDetails node : nodes) {
      if ((useHostname ? node.getHostName() : node.getIpAddress()).equals(clientMachine)) {
        matchingNodes.add(node);
      }
    }
    return !matchingNodes.isEmpty() ? matchingNodes.get(0) :
        getOtherNode(clientMachine, clusterMap);
  }

  private Node getOtherNode(String clientMachine, NetworkTopology clusterMap) {
    try {
      String clientLocation = resolveNodeLocation(clientMachine);
      if (clientLocation != null) {
        Node rack = clusterMap.getNode(clientLocation);
        if (rack instanceof InnerNode) {
          return new NodeImpl(clientMachine, clientLocation,
              (InnerNode) rack, rack.getLevel() + 1,
              NODE_COST_DEFAULT);
        }
      }
    } catch (Exception e) {
      LOG.info("Could not resolve client {}: {}", clientMachine, e.getMessage());
    }
    return null;
  }

  private String resolveNodeLocation(String hostname) {
    List<String> hosts = Collections.singletonList(hostname);
    List<String> resolvedHosts = dnsToSwitchMapping.resolve(hosts);
    if (resolvedHosts != null && !resolvedHosts.isEmpty()) {
      String location = resolvedHosts.get(0);
      LOG.debug("Node {} resolved to location {}", hostname, location);
      return location;
    } else {
      LOG.debug("Node resolution did not yield any result for {}", hostname);
      return null;
    }
  }
}
