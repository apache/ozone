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
package org.apache.hadoop.hdds.security.symmetric;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocol;
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

/**
 * Default implementation of {@link SecretKeySignerClient} that fetches
 * secret keys from SCM. This client implements a background thread that
 * periodically check and get the latest current secret key from SCM.
 */
public class DefaultSecretKeySignerClient implements SecretKeySignerClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultSecretKeySignerClient.class);

  private final SecretKeyProtocol secretKeyProtocol;
  private final AtomicReference<ManagedSecretKey> cache =
      new AtomicReference<>();
  private ScheduledExecutorService executorService;

  public DefaultSecretKeySignerClient(
      SecretKeyProtocol secretKeyProtocol) {
    this.secretKeyProtocol = secretKeyProtocol;
  }

  @Override
  public ManagedSecretKey getCurrentSecretKey() {
    return requireNonNull(cache.get(),
        "SecretKey client must have been initialized already.");
  }

  @Override
  public void refetchSecretKey() {
    // pass duration as ZERO to force a refresh.
    checkAndRefresh(Duration.ZERO);
  }

  @Override
  public void start(ConfigurationSource conf) throws IOException {
    final ManagedSecretKey initialKey =
        secretKeyProtocol.getCurrentSecretKey();
    LOG.info("Initial secret key fetched from SCM: {}.", initialKey);
    cache.set(initialKey);
    scheduleSecretKeyPoller(conf, initialKey.getCreationTime());
  }

  @Override
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

  private void scheduleSecretKeyPoller(ConfigurationSource conf,
                                       Instant initialCreation) {
    Duration rotateDuration = SecretKeyConfig.parseRotateDuration(conf);
    Instant nextRotate = initialCreation.plus(rotateDuration);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("SecretKeyPoller")
        .setDaemon(true)
        .build();
    executorService = Executors.newScheduledThreadPool(1, threadFactory);
    Duration interval = SecretKeyConfig.parseRotateCheckDuration(conf);
    Duration initialDelay = Duration.between(Instant.now(), nextRotate);

    LOG.info("Scheduling SecretKeyPoller with initial delay of {} " +
        "and interval of {}", initialDelay, interval);
    executorService.scheduleAtFixedRate(() -> checkAndRefresh(rotateDuration),
        initialDelay.toMillis(), interval.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  private synchronized void checkAndRefresh(Duration rotateDuration) {
    ManagedSecretKey current = cache.get();
    Instant nextRotate = current.getCreationTime().plus(rotateDuration);
    // when the current key passes the rotation cycle, fetch the next one
    // from SCM.
    if (nextRotate.isBefore(Instant.now())) {
      try {
        ManagedSecretKey newKey = secretKeyProtocol.getCurrentSecretKey();
        if (!newKey.equals(current)) {
          cache.set(newKey);
          LOG.info("New secret key fetched from SCM: {}.", newKey);
        }
      } catch (IOException e) {
        // TODO: emic failure metrics.
        throw new UncheckedIOException(
            "Error fetching current key from SCM", e);
      }
    }
  }
}
