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
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_REFRESH_DURATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_REFRESH_DURATION_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_CHECK_DURATION;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_CHECK_DURATION_DEFAULT;

/**
 * This client implements a background thread which periodically checks and
 * gets the latest network topology schema file from SCM.
 */
public class ScmBlockLocationClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(ScmBlockLocationClient.class);

  private final ScmBlockLocationProtocol scmBlockLocationProtocol;
  private final AtomicReference<String> cache = new AtomicReference<>();
  private ScheduledExecutorService executorService;

  public ScmBlockLocationClient(
      ScmBlockLocationProtocol scmBlockLocationProtocol) {
    this.scmBlockLocationProtocol = scmBlockLocationProtocol;
  }

  public String getTopologyInformation() {
    return requireNonNull(cache.get(),
        "ScmBlockLocationClient must have been initialized already.");
  }

  public void refetchTopologyInformation() {
    checkAndRefresh(Duration.ZERO, Instant.now());
  }

  public void start(ConfigurationSource conf) throws IOException {
    final String initialTopology =
        scmBlockLocationProtocol.getTopologyInformation();
    LOG.info("Initial topology information fetched from SCM: {}.",
        initialTopology);
    cache.set(initialTopology);
    scheduleTopologyPoller(conf, Instant.now());
  }

  public void stop() {
    if (executorService != null) {
      executorService.shutdown();
      try {
        if (executorService.awaitTermination(1, TimeUnit.MINUTES)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted while shutting down executor service.", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  private void scheduleTopologyPoller(ConfigurationSource conf,
                                       Instant initialInvocation) {
    Duration refreshDuration = parseRefreshDuration(conf);
    Instant nextRotate = initialInvocation.plus(refreshDuration);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("NetworkTopologyPoller")
        .setDaemon(true)
        .build();
    executorService = Executors.newScheduledThreadPool(1, threadFactory);
    Duration interval = parseRefreshCheckDuration(conf);
    Duration initialDelay = Duration.between(Instant.now(), nextRotate);

    LOG.info("Scheduling NetworkTopologyPoller with initial delay of {} " +
        "and interval of {}", initialDelay, interval);
    executorService.scheduleAtFixedRate(
        () -> checkAndRefresh(refreshDuration, initialInvocation),
        initialDelay.toMillis(), interval.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  public static Duration parseRefreshDuration(ConfigurationSource conf) {
    long refreshDurationInMs = conf.getTimeDuration(
        OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_REFRESH_DURATION,
        OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_REFRESH_DURATION_DEFAULT,
        TimeUnit.MILLISECONDS);
    return Duration.ofMillis(refreshDurationInMs);
  }

  public static Duration parseRefreshCheckDuration(ConfigurationSource conf) {
    long refreshCheckInMs = conf.getTimeDuration(
        OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_CHECK_DURATION,
        OZONE_SCM_NETWORK_TOPOLOGY_SCHEMA_FILE_CHECK_DURATION_DEFAULT,
        TimeUnit.MILLISECONDS);
    return Duration.ofMillis(refreshCheckInMs);
  }

  private synchronized void checkAndRefresh(Duration refreshDuration,
                                            Instant initialInvocation) {
    String current = cache.get();
    Instant nextRefresh = initialInvocation.plus(refreshDuration);
    if (nextRefresh.isBefore(Instant.now())) {
      try {
        String newTopology = scmBlockLocationProtocol.getTopologyInformation();
        if (!newTopology.equals(current)) {
          cache.set(newTopology);
          LOG.info("Updated network topology schema file fetched from " +
              "SCM: {}.", newTopology);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Error fetching updated network topology schema file from SCM", e);
      }
    }
  }
}
