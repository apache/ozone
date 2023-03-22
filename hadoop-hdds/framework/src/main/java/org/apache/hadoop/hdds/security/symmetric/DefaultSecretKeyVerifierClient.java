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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.security.symmetric.SecretKeyConfig.parseExpiryDuration;
import static org.apache.hadoop.hdds.security.symmetric.SecretKeyConfig.parseRotateDuration;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmSecurityClient;

/**
 * Default implementation of {@link SecretKeyVerifierClient} that fetches
 * SecretKeys remotely via {@link SCMSecurityProtocol}.
 */
public class DefaultSecretKeyVerifierClient implements SecretKeyVerifierClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultSecretKeyVerifierClient.class);

  private final LoadingCache<UUID, Optional<ManagedSecretKey>> cache;

  DefaultSecretKeyVerifierClient(SCMSecurityProtocol scmSecurityProtocol,
                                 ConfigurationSource conf) {
    Duration expiryDuration = parseExpiryDuration(conf);
    Duration rotateDuration = parseRotateDuration(conf);

    // if rotation is 1d, and each keys is valid for 7d before expiring,
    // the expected number valid keys at any time is 7.
    long expectedValidKeys =
        expiryDuration.toMillis() / rotateDuration.toMillis() + 1;
    // However, we want to cache some expired keys as well, to avoid requesting
    // SCM for recently expire secret keys.
    long cacheSize = expectedValidKeys * 2;
    Duration cacheExpiry = expiryDuration.multipliedBy(2);

    CacheLoader<UUID, Optional<ManagedSecretKey>> loader =
        new CacheLoader<UUID, Optional<ManagedSecretKey>>() {
          @Override
          public Optional<ManagedSecretKey> load(UUID id) throws Exception {
            ManagedSecretKey secretKey = scmSecurityProtocol.getSecretKey(id);
            LOG.info("Secret key fetched from SCM: {}", secretKey);
            return Optional.ofNullable(secretKey);
          }
        };

    LOG.info("Initializing secret key cache with size {}, TTL {}",
        cacheSize, expiryDuration);
    cache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .expireAfterWrite(cacheExpiry.toMillis(), TimeUnit.MILLISECONDS)
        .recordStats()
        .build(loader);
  }

  @Override
  public ManagedSecretKey getSecretKey(UUID id) throws SCMSecurityException {
    try {
      return cache.get(id).orElse(null);
    } catch (ExecutionException e) {
      // handle cache load exception.
      if (e.getCause() instanceof IOException) {
        IOException cause = (IOException) e.getCause();
        if (cause instanceof SCMSecurityException) {
          throw (SCMSecurityException) cause;
        } else {
          throw new SCMSecurityException(
              "Error fetching secret key " + id + " from SCM", cause);
        }
      }
      throw new IllegalStateException("Unexpected exception fetching secret " +
          "key " + id + " from SCM", e.getCause());
    }
  }

  public static DefaultSecretKeyVerifierClient create(ConfigurationSource conf)
      throws IOException {
    SCMSecurityProtocol securityProtocol = getScmSecurityClient(conf);
    return new DefaultSecretKeyVerifierClient(securityProtocol, conf);
  }
}
