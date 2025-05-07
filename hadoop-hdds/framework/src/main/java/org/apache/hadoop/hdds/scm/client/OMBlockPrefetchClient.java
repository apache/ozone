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

package org.apache.hadoop.hdds.scm.client;

import static org.apache.hadoop.hdds.scm.net.NetConstants.NODE_COST_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCHED_BLOCKS_EXPIRY_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCHED_BLOCKS_EXPIRY_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MAX_BLOCKS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MAX_BLOCKS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MIN_BLOCKS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_PREFETCH_MIN_BLOCKS_DEFAULT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.Node;
import org.apache.hadoop.hdds.scm.net.NodeImpl;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.TableMapping;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OMBlockPrefetchClient manages block prefetching for efficient write operations.
 * It maintains a queue of allocated blocks per replication configuration and removes expired blocks lazily.
 * The client refills the queue in the background to handle high-throughput scenarios efficiently.
 */
public class OMBlockPrefetchClient {
  private static final Logger LOG = LoggerFactory.getLogger(OMBlockPrefetchClient.class);
  private final ScmBlockLocationProtocol scmBlockLocationProtocol;
  private int maxBlocks;
  private int minBlocks;
  private boolean useHostname;
  private DNSToSwitchMapping dnsToSwitchMapping;
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

  private ExecutorService prefetchExecutor;
  private final AtomicBoolean isPrefetching = new AtomicBoolean(false);

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

  /**
   * Tracks an allocated block to be cached with its cache expiry.
   */
  public static final class ExpiringAllocatedBlock {
    private final AllocatedBlock block;
    private final long expiryTime;

    public ExpiringAllocatedBlock(AllocatedBlock block, long expiryTime) {
      this.block = block;
      this.expiryTime = expiryTime;
    }

    public AllocatedBlock getBlock() {
      return block;
    }

    public long getExpiryTime() {
      return expiryTime;
    }
  }

  public void start(ConfigurationSource conf) throws IOException, InterruptedException, TimeoutException {
    maxBlocks = conf.getInt(OZONE_OM_PREFETCH_MAX_BLOCKS, OZONE_OM_PREFETCH_MAX_BLOCKS_DEFAULT);
    minBlocks = conf.getInt(OZONE_OM_PREFETCH_MIN_BLOCKS, OZONE_OM_PREFETCH_MIN_BLOCKS_DEFAULT);
    expiryDuration = conf.getTimeDuration(OZONE_OM_PREFETCHED_BLOCKS_EXPIRY_INTERVAL,
        OZONE_OM_PREFETCHED_BLOCKS_EXPIRY_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);

    useHostname = conf.getBoolean(HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME,
        HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME_DEFAULT);
    Class<? extends DNSToSwitchMapping> dnsToSwitchMappingClass =
        conf.getClass(ScmConfigKeys.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
            TableMapping.class, DNSToSwitchMapping.class);
    DNSToSwitchMapping newInstance = ReflectionUtils.newInstance(
        dnsToSwitchMappingClass, OzoneConfiguration.of(conf));
    dnsToSwitchMapping =
        ((newInstance instanceof CachedDNSToSwitchMapping) ? newInstance
            : new CachedDNSToSwitchMapping(newInstance));
    metrics = OMBlockPrefetchMetrics.register();
    prefetchExecutor = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "OMBlockPrefetchClient-AsyncPrefetcher");
      t.setDaemon(true);
      return t;
    });
    LOG.info("OMBlockPrefetchClient started with minBlocks={}, maxBlocks={}, expiryDuration={}ms. Prefetch executor " +
            "initialized.", minBlocks, maxBlocks, expiryDuration);
  }

  public void stop() {
    if (prefetchExecutor != null) {
      prefetchExecutor.shutdown();
      try {
        if (prefetchExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          prefetchExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted while shutting down executor service.", e);
        Thread.currentThread().interrupt();
      }
    }
    OMBlockPrefetchMetrics.unregister();
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

      // If there aren't enough blocks in cache, we make a synchronous RPC call to SCM for fetching the remaining blocks
      if (remainingBlocks > 0) {
        List<AllocatedBlock> newBlocks = scmBlockLocationProtocol.allocateBlock(scmBlockSize, remainingBlocks,
            replicationConfig, serviceID, excludeList, clientMachine);
        allocatedBlocks.addAll(newBlocks);
        metrics.incrementCacheMisses();
      } else {
        metrics.incrementCacheHits();
      }

      int queueSize = queue.size();
      if (queueSize < minBlocks) {
        int blocksToPrefetch = minBlocks - queueSize;
        LOG.debug("Cache for {} is below threshold (size: {}, min: {}). Submitting async prefetch task for {} blocks.",
            replicationConfig, queueSize, minBlocks, blocksToPrefetch);
        submitPrefetchTask(scmBlockSize, blocksToPrefetch, replicationConfig, serviceID);
      }

      metrics.addReadFromQueueLatency(Time.monotonicNowNanos() - readStartTime);
      return allocatedBlocks;

    } else {
      LOG.debug("Bypassing cache for {}. Reason: {}", replicationConfig, queue == null ?
          "Unsupported replication config for caching." : "ExcludeList provided.");
      metrics.addReadFromQueueLatency(Time.monotonicNowNanos() - readStartTime);
      metrics.incrementCacheMisses();
      return scmBlockLocationProtocol.allocateBlock(scmBlockSize, numBlocks, replicationConfig, serviceID, excludeList,
          clientMachine);
    }
  }

  private void submitPrefetchTask(long blockSize, int blocksToPrefetch, ReplicationConfig repConfig, String serviceID) {

    if (!isPrefetching.compareAndSet(false, true)) {
      LOG.debug("Prefetch already in progress. Skipping new task for {}.", repConfig);
      return;
    }

    if (prefetchExecutor == null || prefetchExecutor.isShutdown()) {
      LOG.warn("Async prefetch executor is not running or shutdown. Skipping prefetch task for {}.", repConfig);
      return;
    }

    prefetchExecutor.submit(() -> {
      try {
        List<AllocatedBlock> prefetchedBlocks = scmBlockLocationProtocol.allocateBlock(blockSize, blocksToPrefetch,
            repConfig, serviceID, new ExcludeList(), null);
        if (prefetchedBlocks != null && !prefetchedBlocks.isEmpty()) {
          ConcurrentLinkedDeque<ExpiringAllocatedBlock> queue = blockQueueMap.get(repConfig);
          if (queue != null) {
            long expiryTime = System.currentTimeMillis() + expiryDuration;
            int addedCount = 0;
            for (AllocatedBlock block : prefetchedBlocks) {
              if (queue.size() < maxBlocks) {
                queue.offer(new ExpiringAllocatedBlock(block, expiryTime));
                addedCount++;
              }
            }
            LOG.debug("Prefetched {} blocks for replication config {}.", addedCount, repConfig);
          }
        }
      } catch (IOException e) {
        LOG.error("Exception occurred while prefetching blocks.", e);
      } finally {
        isPrefetching.set(false);
      }
    });
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
