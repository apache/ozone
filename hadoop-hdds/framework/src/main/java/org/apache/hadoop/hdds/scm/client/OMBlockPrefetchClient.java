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

  public void prefetchBlocks(long scmBlockSize, int numBlocks,
                             ReplicationConfig replicationConfig,
                             String serviceID, ExcludeList excludeList) {
    ConcurrentLinkedDeque<ExpiringAllocatedBlock> queue = blockQueueMap.get(replicationConfig);
    prefetchExecutor.submit(() -> {
      int currentSize = queue.size();
      int adjustedNumBlocks = Math.min(numBlocks, maxBlocks);
      int blocksToFetch = adjustedNumBlocks - currentSize;

      if (blocksToFetch <= 0) {
        LOG.debug("Queue for replication config {} already has sufficient blocks (current size: {}). " +
            "Skipping prefetch.", replicationConfig, currentSize);
        return;
      }

      long writeStartTime = Time.monotonicNowNanos();
      try {
        List<AllocatedBlock> newBlocks = scmBlockLocationProtocol.allocateBlock(
            scmBlockSize, blocksToFetch, replicationConfig, serviceID, excludeList, null);
        for (AllocatedBlock block : newBlocks) {
          long expiryTime = System.currentTimeMillis() + expiryDuration;
          ExpiringAllocatedBlock expiringBlock = new ExpiringAllocatedBlock(block, expiryTime);
          queue.offer(expiringBlock);
        }
        LOG.info("Prefetched {} blocks for replication config {}.", newBlocks.size(), replicationConfig);
      } catch (IOException e) {
        LOG.error("Failed to prefetch blocks for replication config {}: {}", replicationConfig, e.getMessage(), e);
      } finally {
        metrics.addWriteToQueueLatency(Time.monotonicNowNanos() - writeStartTime);
      }
    });
  }

  public List<AllocatedBlock> getBlocks(long scmBlockSize, int numBlocks, ReplicationConfig replicationConfig,
                                        String serviceID, ExcludeList excludeList, String clientMachine,
                                        NetworkTopology clusterMap) throws IOException {
    long readStartTime = Time.monotonicNowNanos();
    ConcurrentLinkedDeque<ExpiringAllocatedBlock> queue = blockQueueMap.get(replicationConfig);
    List<AllocatedBlock> allocatedBlocks = new ArrayList<>();
    int retrievedBlocksCount = 0;
    boolean allocateBlocksFromSCM = false;
    while (retrievedBlocksCount < numBlocks) {
      ExpiringAllocatedBlock expiringBlock = queue.peek();
      if (expiringBlock == null) {
        break;
      }

      if (System.currentTimeMillis() > expiringBlock.getExpiryTime()) {
        queue.poll();
        continue;
      }

      queue.poll();
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
    if (remainingBlocks > 0) {
      List<AllocatedBlock> newBlocks = scmBlockLocationProtocol.allocateBlock(
          scmBlockSize, remainingBlocks, replicationConfig, serviceID, excludeList, clientMachine);
      allocatedBlocks.addAll(newBlocks);
      metrics.incrementCacheMisses();
      allocateBlocksFromSCM = true;
    }

    metrics.addSortingLogicLatency(Time.monotonicNowNanos() - readStartTime);
    if (!allocateBlocksFromSCM) {
      metrics.incrementCacheHits();
    }
    return allocatedBlocks;
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
