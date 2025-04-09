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

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_NETWORK_TOPOLOGY_REFRESH_DURATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_NETWORK_TOPOLOGY_REFRESH_DURATION_DEFAULT;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.net.InnerNode;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This client implements a background thread which periodically checks and
 * gets the latest network topology cluster tree from SCM.
 */
public class ScmTopologyClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(ScmTopologyClient.class);

  private final ScmBlockLocationProtocol scmBlockLocationProtocol;
  private final AtomicReference<NetworkTopology> cache =
      new AtomicReference<>();
  private ScheduledExecutorService executorService;

  public ScmTopologyClient(
      ScmBlockLocationProtocol scmBlockLocationProtocol) {
    this.scmBlockLocationProtocol = scmBlockLocationProtocol;
  }

  public NetworkTopology getClusterMap() {
    return requireNonNull(cache.get(),
        "ScmBlockLocationClient must have been initialized already.");
  }

  public void start(ConfigurationSource conf) throws IOException {
    final InnerNode initialTopology =
        scmBlockLocationProtocol.getNetworkTopology();
    LOG.info("Initial network topology fetched from SCM: {}.",
        initialTopology);
    cache.set(new NetworkTopologyImpl(conf.get(
        ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE,
        ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_DEFAULT),
        initialTopology));
    scheduleNetworkTopologyPoller(conf, Instant.now());
  }

  public void stop() {
    if (executorService != null) {
      executorService.shutdown();
      try {
        if (executorService.awaitTermination(5, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted while shutting down executor service.", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  private void scheduleNetworkTopologyPoller(ConfigurationSource conf,
                                             Instant initialInvocation) {
    Duration refreshDuration = parseRefreshDuration(conf);
    Instant nextRefresh = initialInvocation.plus(refreshDuration);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("NetworkTopologyPoller")
        .setDaemon(true)
        .build();
    executorService = Executors.newScheduledThreadPool(1, threadFactory);
    Duration initialDelay = Duration.between(Instant.now(), nextRefresh);

    LOG.debug("Scheduling NetworkTopologyPoller with an initial delay of {}.",
        initialDelay);
    executorService.scheduleAtFixedRate(() -> checkAndRefresh(conf),
        initialDelay.toMillis(), refreshDuration.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  public static Duration parseRefreshDuration(ConfigurationSource conf) {
    long refreshDurationInMs = conf.getTimeDuration(
        OZONE_OM_NETWORK_TOPOLOGY_REFRESH_DURATION,
        OZONE_OM_NETWORK_TOPOLOGY_REFRESH_DURATION_DEFAULT,
        TimeUnit.MILLISECONDS);
    return Duration.ofMillis(refreshDurationInMs);
  }

  private synchronized void checkAndRefresh(ConfigurationSource conf) {
    InnerNode current = (InnerNode) cache.get().getNode(ROOT);
    try {
      InnerNode newTopology = scmBlockLocationProtocol.getNetworkTopology();
      if (!newTopology.equals(current)) {
        cache.set(new NetworkTopologyImpl(conf.get(
            ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE,
            ScmConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_DEFAULT),
            newTopology));
        LOG.info("Updated network topology fetched from SCM: {}.", newTopology);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Error fetching updated network topology from SCM", e);
    }
  }
}
