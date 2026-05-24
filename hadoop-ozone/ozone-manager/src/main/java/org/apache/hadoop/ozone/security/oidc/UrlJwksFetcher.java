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

import com.nimbusds.jose.jwk.JWKSet;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.time.Duration;

/**
 * JWKS fetcher backed by a URL.
 */
public final class UrlJwksFetcher implements JwksFetcher {

  private final URL url;
  private final int connectTimeoutMillis;
  private final int readTimeoutMillis;
  private final int sizeLimitBytes;

  public UrlJwksFetcher(URL url) {
    this(url, Duration.ofSeconds(5), Duration.ofSeconds(5), 1024 * 1024);
  }

  public UrlJwksFetcher(URL url, Duration connectTimeout,
      Duration readTimeout, int sizeLimitBytes) {
    if (url == null) {
      throw new IllegalArgumentException("JWKS URL must not be null");
    }
    this.url = url;
    this.connectTimeoutMillis = toMillisInt(connectTimeout,
        "JWKS connect timeout");
    this.readTimeoutMillis = toMillisInt(readTimeout, "JWKS read timeout");
    if (sizeLimitBytes <= 0) {
      throw new IllegalArgumentException(
          "JWKS size limit must be positive");
    }
    this.sizeLimitBytes = sizeLimitBytes;
  }

  @Override
  public JWKSet fetch() throws IOException, ParseException {
    return JWKSet.load(url, connectTimeoutMillis, readTimeoutMillis,
        sizeLimitBytes);
  }

  private static int toMillisInt(Duration value, String name) {
    if (value == null || value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    long millis = value.toMillis();
    if (millis <= 0 || millis > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(name
          + " must be between 1ms and " + Integer.MAX_VALUE + "ms");
    }
    return (int) millis;
  }
}
