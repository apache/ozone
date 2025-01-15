/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.DomainSocketFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.util.OzoneNetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.hdds.DatanodeVersion.SHORT_CIRCUIT_READS;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.PERFORMANCE;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.NO_REPLICA_FOUND;

import org.apache.hadoop.ozone.util.CacheMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * XceiverClientManager is responsible for the lifecycle of XceiverClient
 * instances.  Callers use this class to acquire an XceiverClient instance
 * connected to the desired container pipeline.  When done, the caller also uses
 * this class to release the previously acquired XceiverClient instance.
 *
 *
 * This class caches connection to container for reuse purpose, such that
 * accessing same container frequently will be through the same connection
 * without reestablishing connection. But the connection will be closed if
 * not being used for a period of time.
 */
public class XceiverClientManager extends XceiverClientCreator {
  private static final Logger LOG =
      LoggerFactory.getLogger(XceiverClientManager.class);

  private final Cache<String, XceiverClientSpi> clientCache;
  private final CacheMetrics cacheMetrics;
  private static XceiverClientMetrics metrics;
  private final ConcurrentHashMap<String, DatanodeDetails> localDNCache;

  /**
   * Creates a new XceiverClientManager for non secured ozone cluster.
   * For security enabled ozone cluster, client should use the other constructor
   * with a valid ca certificate in pem string format.
   *
   * @param conf configuration
   */
  public XceiverClientManager(ConfigurationSource conf) throws IOException {
    this(conf, conf.getObject(ScmClientConfig.class), null);
  }

  public XceiverClientManager(ConfigurationSource conf,
      ScmClientConfig clientConf,
      ClientTrustManager trustManager) throws IOException {
    super(conf, trustManager);
    Preconditions.checkNotNull(clientConf);
    Preconditions.checkNotNull(conf);
    long staleThresholdMs = clientConf.getStaleThreshold(MILLISECONDS);

    this.clientCache = CacheBuilder.newBuilder()
        .recordStats()
        .expireAfterAccess(staleThresholdMs, MILLISECONDS)
        .maximumSize(clientConf.getMaxSize())
        .removalListener(
            new RemovalListener<String, XceiverClientSpi>() {
            @Override
            public void onRemoval(
                RemovalNotification<String, XceiverClientSpi>
                  removalNotification) {
              synchronized (clientCache) {
                // Mark the entry as evicted
                XceiverClientSpi info = removalNotification.getValue();
                info.setEvicted();
              }
            }
          }).build();

    cacheMetrics = CacheMetrics.create(clientCache, this);
    this.localDNCache = new ConcurrentHashMap<>();
  }

  @VisibleForTesting
  public Cache<String, XceiverClientSpi> getClientCache() {
    return clientCache;
  }

  /**
   * Acquires a XceiverClientSpi connected to a container for read.
   *
   * If there is already a cached XceiverClientSpi, simply return
   * the cached otherwise create a new one.
   *
   * @param pipeline the container pipeline for the client connection
   * @param allowShortCircuit create a short-circuit read client or not if applicable
   * @return XceiverClientSpi connected to a container
   * @throws IOException if a XceiverClientSpi cannot be acquired
   */
  @Override
  public XceiverClientSpi acquireClientForReadData(Pipeline pipeline, boolean allowShortCircuit)
      throws IOException {
    return acquireClient(pipeline, false, allowShortCircuit);
  }

  /**
   * Acquires a XceiverClientSpi connected to a container capable of
   * storing the specified key.
   *
   * If there is already a cached XceiverClientSpi, simply return
   * the cached otherwise create a new one.
   *
   * @param pipeline the container pipeline for the client connection
   * @return XceiverClientSpi connected to a container
   * @throws IOException if a XceiverClientSpi cannot be acquired
   */
  @Override
  public XceiverClientSpi acquireClient(Pipeline pipeline,
      boolean topologyAware, boolean allowShortCircuit) throws IOException {
    Preconditions.checkNotNull(pipeline);
    Preconditions.checkArgument(pipeline.getNodes() != null);
    Preconditions.checkArgument(!pipeline.getNodes().isEmpty(),
        NO_REPLICA_FOUND);

    synchronized (clientCache) {
      XceiverClientSpi info = getClient(pipeline, topologyAware, allowShortCircuit);
      info.incrementReference();
      return info;
    }
  }

  @Override
  public XceiverClientSpi acquireClient(Pipeline pipeline, boolean topologyAware) throws IOException {
    return acquireClient(pipeline, topologyAware, false);
  }

  @Override
  public void releaseClient(XceiverClientSpi client, boolean invalidateClient,
      boolean topologyAware) {
    Preconditions.checkNotNull(client);
    synchronized (clientCache) {
      client.decrementReference();
      if (invalidateClient) {
        Pipeline pipeline = client.getPipeline();
        String key = getPipelineCacheKey(pipeline, topologyAware, client instanceof XceiverClientShortCircuit);
        XceiverClientSpi cachedClient = clientCache.getIfPresent(key);
        if (cachedClient == client) {
          clientCache.invalidate(key);
        }
      }
    }
  }

  protected XceiverClientSpi getClient(Pipeline pipeline, boolean topologyAware, boolean allowShortCircuit)
      throws IOException {
    try {
      // create different client different pipeline node based on network topology
      String key = getPipelineCacheKey(pipeline, topologyAware, allowShortCircuit);
      return clientCache.get(key, () -> newClient(pipeline, localDNCache.get(key)));
    } catch (Exception e) {
      throw new IOException(
          "Exception getting XceiverClient: " + e, e);
    }
  }

  private String getPipelineCacheKey(Pipeline pipeline, boolean topologyAware, boolean allowShortCircuit) {
    String key = pipeline.getId().getId().toString() + "-" + pipeline.getType();
    boolean isEC = pipeline.getType() == HddsProtos.ReplicationType.EC;
    DatanodeDetails localDN = null;

    if ((!isEC) && allowShortCircuit && isShortCircuitEnabled()) {
      int port = 0;
      InetSocketAddress localAddr = null;
      for (DatanodeDetails dn : pipeline.getNodes()) {
        // read port from the data node, on failure use default configured port.
        port = dn.getPort(DatanodeDetails.Port.Name.STANDALONE).getValue();
        InetSocketAddress addr = NetUtils.createSocketAddr(dn.getIpAddress(), port);
        if (OzoneNetUtils.isAddressLocal(addr) &&
            dn.getCurrentVersion() >= SHORT_CIRCUIT_READS.toProtoValue()) {
          localAddr = addr;
          localDN = dn;
          break;
        }
      }
      if (localAddr != null) {
        // Find a local DN and short circuit read is enabled
        key += "@" + localAddr.getHostName() + ":" + port + "/" + DomainSocketFactory.FEATURE_FLAG;
      }
    }

    if (localDN == null && (topologyAware || isEC)) {
      try {
        DatanodeDetails closestNode = pipeline.getClosestNode();
        // Pipeline cache key uses host:port suffix to handle
        // both EC, Ratis, and Standalone client.
        //
        // For EC client, the host:port cache key is needed
        // so that there is a different cache key for each node in
        // a block group.
        //
        // For Ratis and Standalone client, the host:port cache key is needed
        // to handle the possibility of two datanodes sharing the same machine.
        // While normally one machine only hosts one datanode service,
        // this situation might arise in tests.
        //
        // Standalone port is chosen since all datanodes should have a
        // standalone port regardless of version and this port should not
        // have any collisions.
        key += closestNode.getHostName() + closestNode.getStandalonePort();
      } catch (IOException e) {
        LOG.error("Failed to get closest node to create pipeline cache key:" +
            e.getMessage());
      }
    }

    if (isSecurityEnabled()) {
      // Append user short name to key to prevent a different user
      // from using same instance of xceiverClient.
      try {
        key = UserGroupInformation.getCurrentUser().getShortUserName() + "@" + key;
      } catch (IOException e) {
        LOG.error("Failed to get current user to create pipeline cache key:" +
            e.getMessage());
      }
    }

    if (localDN != null) {
      localDNCache.put(key, localDN);
    }
    return key;
  }

  /**
   * Close and remove all the cached clients.
   */
  @Override
  public void close() {
    super.close();
    //closing is done through RemovalListener
    clientCache.invalidateAll();
    clientCache.cleanUp();
    if (LOG.isDebugEnabled()) {
      LOG.debug("XceiverClient cache stats: {}", clientCache.stats());
    }
    localDNCache.clear();
    cacheMetrics.unregister();

    if (metrics != null) {
      metrics.unRegister();
    }
  }

  /**
   * Get xceiver client metric.
   */
  public static synchronized XceiverClientMetrics getXceiverClientMetrics() {
    if (metrics == null) {
      metrics = XceiverClientMetrics.create();
    }

    return metrics;
  }

  /**
   * Reset xceiver client metric.
   */
  public static synchronized void resetXceiverClientMetrics() {
    if (metrics != null) {
      metrics.reset();
    }
  }

  /**
   * Configuration for HDDS client.
   */
  @ConfigGroup(prefix = "scm.container.client")
  public static class ScmClientConfig {

    @Config(key = "max.size",
        defaultValue = "256",
        tags = {OZONE, PERFORMANCE},
        description =
            "Controls the maximum number of connections that are cached via"
                + " client connection pooling. If the number of connections"
                + " exceed this count, then the oldest idle connection is "
                + "evicted."
    )
    private int maxSize;

    @Config(key = "idle.threshold",
        type = ConfigType.TIME, timeUnit = MILLISECONDS,
        defaultValue = "10s",
        tags = {OZONE, PERFORMANCE},
        description =
            "In the standalone pipelines, the SCM clients use netty to "
                + " communicate with the container. It also uses connection "
                + "pooling"
                + " to reduce client side overheads. This allows a connection"
                + " to"
                + " stay idle for a while before the connection is closed."
    )
    private long staleThreshold;

    public long getStaleThreshold(TimeUnit unit) {
      return unit.convert(staleThreshold, MILLISECONDS);
    }


    public int getMaxSize() {
      return maxSize;
    }

    @VisibleForTesting
    public void setMaxSize(int maxSize) {
      this.maxSize = maxSize;
    }

    public void setStaleThreshold(long threshold) {
      this.staleThreshold = threshold;
    }

  }

  /**
   * Builder of ScmClientConfig.
   */
  public static class XceiverClientManagerConfigBuilder {

    private int maxCacheSize;
    private long staleThresholdMs;

    public XceiverClientManagerConfigBuilder setMaxCacheSize(int maxCacheSize) {
      this.maxCacheSize = maxCacheSize;
      return this;
    }

    public XceiverClientManagerConfigBuilder setStaleThresholdMs(
        long staleThresholdMs) {
      this.staleThresholdMs = staleThresholdMs;
      return this;
    }

    public ScmClientConfig build() {
      ScmClientConfig clientConfig = new ScmClientConfig();
      clientConfig.setMaxSize(this.maxCacheSize);
      clientConfig.setStaleThreshold(this.staleThresholdMs);
      return clientConfig;
    }
  }

}
