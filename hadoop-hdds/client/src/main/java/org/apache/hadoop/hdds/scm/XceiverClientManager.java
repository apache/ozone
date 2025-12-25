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

package org.apache.hadoop.hdds.scm;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.PERFORMANCE;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.NO_REPLICA_FOUND;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.util.CacheMetrics;
import org.apache.hadoop.security.UserGroupInformation;
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
    Objects.requireNonNull(clientConf, "clientConf == null");
    Objects.requireNonNull(conf, "conf == null");
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
  }

  @VisibleForTesting
  public Cache<String, XceiverClientSpi> getClientCache() {
    return clientCache;
  }

  /**
   * {@inheritDoc}
   *
   * If there is already a cached XceiverClientSpi, simply return
   * the cached otherwise create a new one.
   */
  @Override
  public XceiverClientSpi acquireClient(Pipeline pipeline,
      boolean topologyAware) throws IOException {
    Objects.requireNonNull(pipeline, "pipeline == null");
    Preconditions.checkArgument(pipeline.getNodes() != null);
    Preconditions.checkArgument(!pipeline.getNodes().isEmpty(),
        NO_REPLICA_FOUND);

    synchronized (clientCache) {
      XceiverClientSpi info = getClient(pipeline, topologyAware);
      info.incrementReference();
      return info;
    }
  }

  @Override
  public void releaseClient(XceiverClientSpi client, boolean invalidateClient,
      boolean topologyAware) {
    Objects.requireNonNull(client, "client == null");
    synchronized (clientCache) {
      client.decrementReference();
      if (invalidateClient) {
        Pipeline pipeline = client.getPipeline();
        String key = getPipelineCacheKey(pipeline, topologyAware);
        XceiverClientSpi cachedClient = clientCache.getIfPresent(key);
        if (cachedClient == client) {
          clientCache.invalidate(key);
        }
      }
    }
  }

  protected XceiverClientSpi getClient(Pipeline pipeline, boolean topologyAware)
      throws IOException {
    try {
      // create different client different pipeline node based on
      // network topology
      String key = getPipelineCacheKey(pipeline, topologyAware);
      return clientCache.get(key, () -> newClient(pipeline));
    } catch (Exception e) {
      throw new IOException(
          "Exception getting XceiverClient: " + e, e);
    }
  }

  private String getPipelineCacheKey(Pipeline pipeline,
                                     boolean topologyAware) {
    String key = pipeline.getId().getId().toString() + pipeline.getType();
    boolean isEC = pipeline.getType() == HddsProtos.ReplicationType.EC;
    if (topologyAware || isEC) {
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
        key += UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        LOG.error("Failed to get current user to create pipeline cache key:" +
            e.getMessage());
      }
    }
    return key;
  }

  /**
   * Close and remove all the cached clients.
   */
  @Override
  public void close() {
    //closing is done through RemovalListener
    clientCache.invalidateAll();
    clientCache.cleanUp();
    if (LOG.isDebugEnabled()) {
      LOG.debug("XceiverClient cache stats: {}", clientCache.stats());
    }
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

    @Config(key = "scm.container.client.max.size",
        defaultValue = "256",
        tags = {OZONE, PERFORMANCE},
        description =
            "Controls the maximum number of connections that are cached via"
                + " client connection pooling. If the number of connections"
                + " exceed this count, then the oldest idle connection is "
                + "evicted."
    )
    private int maxSize;

    @Config(key = "scm.container.client.idle.threshold",
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
