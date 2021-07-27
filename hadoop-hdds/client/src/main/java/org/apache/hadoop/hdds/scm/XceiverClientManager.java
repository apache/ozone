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

import java.io.Closeable;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.PERFORMANCE;
import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.NO_REPLICA_FOUND;
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
public class XceiverClientManager implements Closeable, XceiverClientFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(XceiverClientManager.class);
  //TODO : change this to SCM configuration class
  private final ConfigurationSource conf;
  private final Cache<String, XceiverClientSpi> clientCache;
  private List<X509Certificate> caCerts;

  private static XceiverClientMetrics metrics;
  private boolean isSecurityEnabled;
  private final boolean topologyAwareRead;
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
      List<X509Certificate> caCerts) throws IOException {
    Preconditions.checkNotNull(clientConf);
    Preconditions.checkNotNull(conf);
    long staleThresholdMs = clientConf.getStaleThreshold(MILLISECONDS);
    this.conf = conf;
    this.isSecurityEnabled = OzoneSecurityUtil.isSecurityEnabled(conf);
    if (isSecurityEnabled) {
      Preconditions.checkNotNull(caCerts);
      this.caCerts = caCerts;
    }

    this.clientCache = CacheBuilder.newBuilder()
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
    topologyAwareRead = conf.getBoolean(
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
        OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_DEFAULT);
  }

  @VisibleForTesting
  public Cache<String, XceiverClientSpi> getClientCache() {
    return clientCache;
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
  public XceiverClientSpi acquireClient(Pipeline pipeline)
      throws IOException {
    return acquireClient(pipeline, false);
  }

  /**
   * Acquires a XceiverClientSpi connected to a container for read.
   *
   * If there is already a cached XceiverClientSpi, simply return
   * the cached otherwise create a new one.
   *
   * @param pipeline the container pipeline for the client connection
   * @return XceiverClientSpi connected to a container
   * @throws IOException if a XceiverClientSpi cannot be acquired
   */
  @Override
  public XceiverClientSpi acquireClientForReadData(Pipeline pipeline)
      throws IOException {
    return acquireClient(pipeline, true);
  }

  private XceiverClientSpi acquireClient(Pipeline pipeline, boolean read)
      throws IOException {
    Preconditions.checkNotNull(pipeline);
    Preconditions.checkArgument(pipeline.getNodes() != null);
    Preconditions.checkArgument(!pipeline.getNodes().isEmpty(),
        NO_REPLICA_FOUND);

    synchronized (clientCache) {
      XceiverClientSpi info = getClient(pipeline, read);
      info.incrementReference();
      return info;
    }
  }

  /**
   * Releases a XceiverClientSpi after use.
   *
   * @param client client to release
   * @param invalidateClient if true, invalidates the client in cache
   */
  @Override
  public void releaseClient(XceiverClientSpi client, boolean invalidateClient) {
    releaseClient(client, invalidateClient, false);
  }

  /**
   * Releases a read XceiverClientSpi after use.
   *
   * @param client client to release
   * @param invalidateClient if true, invalidates the client in cache
   */
  @Override
  public void releaseClientForReadData(XceiverClientSpi client,
      boolean invalidateClient) {
    releaseClient(client, invalidateClient, true);
  }

  private void releaseClient(XceiverClientSpi client, boolean invalidateClient,
      boolean read) {
    Preconditions.checkNotNull(client);
    synchronized (clientCache) {
      client.decrementReference();
      if (invalidateClient) {
        Pipeline pipeline = client.getPipeline();
        String key = getPipelineCacheKey(pipeline, read);
        XceiverClientSpi cachedClient = clientCache.getIfPresent(key);
        if (cachedClient == client) {
          clientCache.invalidate(key);
        }
      }
    }
  }

  private XceiverClientSpi getClient(Pipeline pipeline, boolean forRead)
      throws IOException {
    HddsProtos.ReplicationType type = pipeline.getType();
    try {
      // create different client for read different pipeline node based on
      // network topology
      String key = getPipelineCacheKey(pipeline, forRead);
      // Append user short name to key to prevent a different user
      // from using same instance of xceiverClient.
      key = isSecurityEnabled ?
          key + UserGroupInformation.getCurrentUser().getShortUserName() : key;
      return clientCache.get(key, new Callable<XceiverClientSpi>() {
        @Override
          public XceiverClientSpi call() throws Exception {
            XceiverClientSpi client = null;
            switch (type) {
            case RATIS:
              client = XceiverClientRatis.newXceiverClientRatis(pipeline, conf,
                  caCerts);
              break;
            case STAND_ALONE:
              client = new XceiverClientGrpc(pipeline, conf, caCerts);
              break;
            case CHAINED:
            default:
              throw new IOException("not implemented" + pipeline.getType());
            }
            client.connect();
            return client;
          }
        });
    } catch (Exception e) {
      throw new IOException(
          "Exception getting XceiverClient: " + e.toString(), e);
    }
  }

  private String getPipelineCacheKey(Pipeline pipeline, boolean forRead) {
    String key = pipeline.getId().getId().toString() + pipeline.getType();
    if (topologyAwareRead && forRead) {
      try {
        key += pipeline.getClosestNode().getHostName();
      } catch (IOException e) {
        LOG.error("Failed to get closest node to create pipeline cache key:" +
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

  }

}
