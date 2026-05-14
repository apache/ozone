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

package org.apache.hadoop.ozone.security.oidc;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import java.io.IOException;
import java.text.ParseException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Thread-safe JWKS cache with refresh-on-unknown-kid semantics.
 */
public final class CachingJwksProvider implements JwksProvider {

  private static final Duration DEFAULT_UNKNOWN_KID_REFRESH_DEBOUNCE =
      Duration.ofSeconds(5);

  private final JwksFetcher fetcher;
  private final Duration refreshInterval;
  private final Duration unknownKidRefreshDebounce;
  private final Clock clock;
  private volatile JWKSet jwkSet;
  private volatile Instant loadedAt = Instant.EPOCH;
  private volatile Instant lastUnknownKidRefreshAt = Instant.EPOCH;

  public CachingJwksProvider(JwksFetcher fetcher, Duration refreshInterval) {
    this(fetcher, refreshInterval, Clock.systemUTC());
  }

  CachingJwksProvider(JwksFetcher fetcher, Duration refreshInterval,
      Clock clock) {
    this(fetcher, refreshInterval, DEFAULT_UNKNOWN_KID_REFRESH_DEBOUNCE,
        clock);
  }

  CachingJwksProvider(JwksFetcher fetcher, Duration refreshInterval,
      Duration unknownKidRefreshDebounce, Clock clock) {
    if (fetcher == null) {
      throw new IllegalArgumentException("JWKS fetcher must not be null");
    }
    if (refreshInterval == null || refreshInterval.isNegative()) {
      throw new IllegalArgumentException(
          "JWKS refresh interval must not be negative");
    }
    if (unknownKidRefreshDebounce == null
        || unknownKidRefreshDebounce.isNegative()) {
      throw new IllegalArgumentException(
          "Unknown kid refresh debounce must not be negative");
    }
    this.fetcher = fetcher;
    this.refreshInterval = refreshInterval;
    this.unknownKidRefreshDebounce = unknownKidRefreshDebounce;
    this.clock = clock;
  }

  @Override
  public List<JWK> getKeys(String keyId) throws OidcAuthenticationException {
    refreshIfNeeded(false);
    List<JWK> keys = findKeys(jwkSet, keyId);
    if (keys.isEmpty() && keyId != null && !keyId.trim().isEmpty()) {
      refreshForUnknownKidIfNeeded();
      keys = findKeys(jwkSet, keyId);
    }
    return keys;
  }

  private void refreshIfNeeded(boolean force)
      throws OidcAuthenticationException {
    Instant now = clock.instant();
    JWKSet snapshot = jwkSet;
    if (!force && snapshot != null
        && now.isBefore(loadedAt.plus(refreshInterval))) {
      return;
    }

    synchronized (this) {
      now = clock.instant();
      snapshot = jwkSet;
      if (!force && snapshot != null
          && now.isBefore(loadedAt.plus(refreshInterval))) {
        return;
      }
      try {
        jwkSet = fetcher.fetch();
        loadedAt = now;
      } catch (IOException | ParseException e) {
        throw new OidcAuthenticationException(
            "Unable to refresh OIDC JWKS", e);
      }
    }
  }

  private void refreshForUnknownKidIfNeeded()
      throws OidcAuthenticationException {
    Instant now = clock.instant();
    if (now.isBefore(lastUnknownKidRefreshAt.plus(
        unknownKidRefreshDebounce))) {
      return;
    }

    synchronized (this) {
      now = clock.instant();
      if (now.isBefore(lastUnknownKidRefreshAt.plus(
          unknownKidRefreshDebounce))) {
        return;
      }
      try {
        jwkSet = fetcher.fetch();
        loadedAt = now;
        lastUnknownKidRefreshAt = now;
      } catch (IOException | ParseException e) {
        throw new OidcAuthenticationException(
            "Unable to refresh OIDC JWKS", e);
      }
    }
  }

  private static List<JWK> findKeys(JWKSet set, String keyId) {
    if (set == null) {
      return Collections.emptyList();
    }
    if (keyId == null || keyId.trim().isEmpty()) {
      return Collections.unmodifiableList(new ArrayList<>(set.getKeys()));
    }
    JWK key = set.getKeyByKeyId(keyId);
    if (key == null) {
      return Collections.emptyList();
    }
    return Collections.singletonList(key);
  }
}
