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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ALLOW_INSECURE_HTTP_FOR_TESTS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ALLOW_INSECURE_HTTP_FOR_TESTS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_AUDIENCE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_AUDIENCE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_CLOCK_SKEW;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_CLOCK_SKEW_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_GROUPS_CLAIM;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_GROUPS_CLAIM_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ISSUER_URI;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ISSUER_URI_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_JWKS_CONNECT_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_JWKS_CONNECT_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_JWKS_READ_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_JWKS_READ_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_JWKS_REFRESH_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_JWKS_REFRESH_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_JWKS_SIZE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_JWKS_SIZE_LIMIT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_JWKS_URI;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_JWKS_URI_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_REQUIRE_HTTPS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_REQUIRE_HTTPS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ROLES_CLAIM;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_ROLES_CLAIM_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_SUBJECT_CLAIM;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_SUBJECT_CLAIM_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_USERNAME_CLAIM;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_STS_WEB_IDENTITY_USERNAME_CLAIM_DEFAULT;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;

/**
 * Configuration for the experimental OIDC identity provider.
 */
public final class OidcConfig {

  private final boolean enabled;
  private final String issuerUri;
  private final String jwksUri;
  private final String audience;
  private final String usernameClaim;
  private final String subjectClaim;
  private final String groupsClaim;
  private final String rolesClaim;
  private final Duration clockSkew;
  private final Duration jwksRefreshInterval;
  private final Duration jwksConnectTimeout;
  private final Duration jwksReadTimeout;
  private final int jwksSizeLimit;
  private final boolean requireHttps;
  private final boolean allowInsecureHttpForTests;

  private OidcConfig(Builder builder) {
    this.enabled = builder.enabled;
    this.issuerUri = trimToEmpty(builder.issuerUri);
    this.jwksUri = trimToEmpty(builder.jwksUri);
    this.audience = trimToEmpty(builder.audience);
    this.usernameClaim = requireNonBlank(builder.usernameClaim,
        OZONE_STS_WEB_IDENTITY_USERNAME_CLAIM);
    this.subjectClaim = requireNonBlank(builder.subjectClaim,
        OZONE_STS_WEB_IDENTITY_SUBJECT_CLAIM);
    this.groupsClaim = requireNonBlank(builder.groupsClaim,
        OZONE_STS_WEB_IDENTITY_GROUPS_CLAIM);
    this.rolesClaim = requireNonBlank(builder.rolesClaim,
        OZONE_STS_WEB_IDENTITY_ROLES_CLAIM);
    this.clockSkew = requireNonNegative(builder.clockSkew,
        OZONE_STS_WEB_IDENTITY_CLOCK_SKEW);
    this.jwksRefreshInterval = requireNonNegative(builder.jwksRefreshInterval,
        OZONE_STS_WEB_IDENTITY_JWKS_REFRESH_INTERVAL);
    this.jwksConnectTimeout = requirePositive(builder.jwksConnectTimeout,
        OZONE_STS_WEB_IDENTITY_JWKS_CONNECT_TIMEOUT);
    this.jwksReadTimeout = requirePositive(builder.jwksReadTimeout,
        OZONE_STS_WEB_IDENTITY_JWKS_READ_TIMEOUT);
    this.jwksSizeLimit = requirePositive(builder.jwksSizeLimit,
        OZONE_STS_WEB_IDENTITY_JWKS_SIZE_LIMIT);
    this.requireHttps = builder.requireHttps;
    this.allowInsecureHttpForTests = builder.allowInsecureHttpForTests;
  }

  public static OidcConfig from(ConfigurationSource conf) {
    OidcConfig config = newBuilder()
        .setEnabled(conf.getBoolean(OZONE_STS_WEB_IDENTITY_ENABLED,
            OZONE_STS_WEB_IDENTITY_ENABLED_DEFAULT))
        .setIssuerUri(conf.getTrimmed(OZONE_STS_WEB_IDENTITY_ISSUER_URI,
            OZONE_STS_WEB_IDENTITY_ISSUER_URI_DEFAULT))
        .setJwksUri(conf.getTrimmed(OZONE_STS_WEB_IDENTITY_JWKS_URI,
            OZONE_STS_WEB_IDENTITY_JWKS_URI_DEFAULT))
        .setAudience(conf.getTrimmed(OZONE_STS_WEB_IDENTITY_AUDIENCE,
            OZONE_STS_WEB_IDENTITY_AUDIENCE_DEFAULT))
        .setUsernameClaim(conf.getTrimmed(
            OZONE_STS_WEB_IDENTITY_USERNAME_CLAIM,
            OZONE_STS_WEB_IDENTITY_USERNAME_CLAIM_DEFAULT))
        .setSubjectClaim(conf.getTrimmed(
            OZONE_STS_WEB_IDENTITY_SUBJECT_CLAIM,
            OZONE_STS_WEB_IDENTITY_SUBJECT_CLAIM_DEFAULT))
        .setGroupsClaim(conf.getTrimmed(OZONE_STS_WEB_IDENTITY_GROUPS_CLAIM,
            OZONE_STS_WEB_IDENTITY_GROUPS_CLAIM_DEFAULT))
        .setRolesClaim(conf.getTrimmed(OZONE_STS_WEB_IDENTITY_ROLES_CLAIM,
            OZONE_STS_WEB_IDENTITY_ROLES_CLAIM_DEFAULT))
        .setClockSkew(duration(conf, OZONE_STS_WEB_IDENTITY_CLOCK_SKEW,
            OZONE_STS_WEB_IDENTITY_CLOCK_SKEW_DEFAULT))
        .setJwksRefreshInterval(duration(conf,
            OZONE_STS_WEB_IDENTITY_JWKS_REFRESH_INTERVAL,
            OZONE_STS_WEB_IDENTITY_JWKS_REFRESH_INTERVAL_DEFAULT))
        .setJwksConnectTimeout(duration(conf,
            OZONE_STS_WEB_IDENTITY_JWKS_CONNECT_TIMEOUT,
            OZONE_STS_WEB_IDENTITY_JWKS_CONNECT_TIMEOUT_DEFAULT))
        .setJwksReadTimeout(duration(conf,
            OZONE_STS_WEB_IDENTITY_JWKS_READ_TIMEOUT,
            OZONE_STS_WEB_IDENTITY_JWKS_READ_TIMEOUT_DEFAULT))
        .setJwksSizeLimit(storageSize(conf,
            OZONE_STS_WEB_IDENTITY_JWKS_SIZE_LIMIT,
            OZONE_STS_WEB_IDENTITY_JWKS_SIZE_LIMIT_DEFAULT))
        .setRequireHttps(conf.getBoolean(OZONE_STS_WEB_IDENTITY_REQUIRE_HTTPS,
            OZONE_STS_WEB_IDENTITY_REQUIRE_HTTPS_DEFAULT))
        .setAllowInsecureHttpForTests(conf.getBoolean(
            OZONE_STS_WEB_IDENTITY_ALLOW_INSECURE_HTTP_FOR_TESTS,
            OZONE_STS_WEB_IDENTITY_ALLOW_INSECURE_HTTP_FOR_TESTS_DEFAULT))
        .build();
    if (config.isEnabled()) {
      config.validateForProvider();
    }
    return config;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public boolean isEnabled() {
    return enabled;
  }

  public String getIssuerUri() {
    return issuerUri;
  }

  public String getJwksUri() {
    return jwksUri;
  }

  public String getAudience() {
    return audience;
  }

  public String getUsernameClaim() {
    return usernameClaim;
  }

  public String getSubjectClaim() {
    return subjectClaim;
  }

  public String getGroupsClaim() {
    return groupsClaim;
  }

  public String getRolesClaim() {
    return rolesClaim;
  }

  public Duration getClockSkew() {
    return clockSkew;
  }

  public Duration getJwksRefreshInterval() {
    return jwksRefreshInterval;
  }

  public Duration getJwksConnectTimeout() {
    return jwksConnectTimeout;
  }

  public Duration getJwksReadTimeout() {
    return jwksReadTimeout;
  }

  public int getJwksSizeLimit() {
    return jwksSizeLimit;
  }

  public boolean isRequireHttps() {
    return requireHttps;
  }

  public boolean isAllowInsecureHttpForTests() {
    return allowInsecureHttpForTests;
  }

  void validateForProvider() {
    requireNonBlank(issuerUri, OZONE_STS_WEB_IDENTITY_ISSUER_URI);
    requireNonBlank(audience, OZONE_STS_WEB_IDENTITY_AUDIENCE);

    if (requireHttps && !allowInsecureHttpForTests) {
      requireHttpsUri(issuerUri, OZONE_STS_WEB_IDENTITY_ISSUER_URI);
      if (!jwksUri.isEmpty()) {
        requireHttpsUri(jwksUri, OZONE_STS_WEB_IDENTITY_JWKS_URI);
      }
    }
  }

  private static Duration duration(ConfigurationSource conf, String key,
      String defaultValue) {
    return Duration.ofMillis(conf.getTimeDuration(key, defaultValue,
        TimeUnit.MILLISECONDS));
  }

  private static int storageSize(ConfigurationSource conf, String key,
      String defaultValue) {
    double bytes = conf.getStorageSize(key, defaultValue, StorageUnit.BYTES);
    if (bytes > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(key + " must not exceed "
          + Integer.MAX_VALUE + " bytes");
    }
    return (int) bytes;
  }

  private static Duration requireNonNegative(Duration value, String key) {
    if (value == null || value.isNegative()) {
      throw new IllegalArgumentException(key + " must not be negative");
    }
    return value;
  }

  private static Duration requirePositive(Duration value, String key) {
    if (value == null || value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(key + " must be positive");
    }
    return value;
  }

  private static int requirePositive(int value, String key) {
    if (value <= 0) {
      throw new IllegalArgumentException(key + " must be positive");
    }
    return value;
  }

  private static String requireNonBlank(String value, String key) {
    String trimmed = trimToEmpty(value);
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException(key + " must not be empty");
    }
    return trimmed;
  }

  private static String trimToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static void requireHttpsUri(String value, String key) {
    URI uri;
    try {
      uri = new URI(value);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(key + " is not a valid URI", e);
    }
    if (!"https".equalsIgnoreCase(uri.getScheme())) {
      throw new IllegalArgumentException(key + " must use https");
    }
  }

  /**
   * Builder for {@link OidcConfig}.
   */
  public static final class Builder {

    private boolean enabled = OZONE_STS_WEB_IDENTITY_ENABLED_DEFAULT;
    private String issuerUri = OZONE_STS_WEB_IDENTITY_ISSUER_URI_DEFAULT;
    private String jwksUri = OZONE_STS_WEB_IDENTITY_JWKS_URI_DEFAULT;
    private String audience = OZONE_STS_WEB_IDENTITY_AUDIENCE_DEFAULT;
    private String usernameClaim =
        OZONE_STS_WEB_IDENTITY_USERNAME_CLAIM_DEFAULT;
    private String subjectClaim =
        OZONE_STS_WEB_IDENTITY_SUBJECT_CLAIM_DEFAULT;
    private String groupsClaim = OZONE_STS_WEB_IDENTITY_GROUPS_CLAIM_DEFAULT;
    private String rolesClaim = OZONE_STS_WEB_IDENTITY_ROLES_CLAIM_DEFAULT;
    private Duration clockSkew = Duration.ofSeconds(60);
    private Duration jwksRefreshInterval = Duration.ofMinutes(10);
    private Duration jwksConnectTimeout = Duration.ofSeconds(5);
    private Duration jwksReadTimeout = Duration.ofSeconds(5);
    private int jwksSizeLimit = 1024 * 1024;
    private boolean requireHttps = OZONE_STS_WEB_IDENTITY_REQUIRE_HTTPS_DEFAULT;
    private boolean allowInsecureHttpForTests =
        OZONE_STS_WEB_IDENTITY_ALLOW_INSECURE_HTTP_FOR_TESTS_DEFAULT;

    private Builder() {
    }

    public Builder setEnabled(boolean value) {
      this.enabled = value;
      return this;
    }

    public Builder setIssuerUri(String value) {
      this.issuerUri = value;
      return this;
    }

    public Builder setJwksUri(String value) {
      this.jwksUri = value;
      return this;
    }

    public Builder setAudience(String value) {
      this.audience = value;
      return this;
    }

    public Builder setUsernameClaim(String value) {
      this.usernameClaim = value;
      return this;
    }

    public Builder setSubjectClaim(String value) {
      this.subjectClaim = value;
      return this;
    }

    public Builder setGroupsClaim(String value) {
      this.groupsClaim = value;
      return this;
    }

    public Builder setRolesClaim(String value) {
      this.rolesClaim = value;
      return this;
    }

    public Builder setClockSkew(Duration value) {
      this.clockSkew = value;
      return this;
    }

    public Builder setJwksRefreshInterval(Duration value) {
      this.jwksRefreshInterval = value;
      return this;
    }

    public Builder setJwksConnectTimeout(Duration value) {
      this.jwksConnectTimeout = value;
      return this;
    }

    public Builder setJwksReadTimeout(Duration value) {
      this.jwksReadTimeout = value;
      return this;
    }

    public Builder setJwksSizeLimit(int value) {
      this.jwksSizeLimit = value;
      return this;
    }

    public Builder setRequireHttps(boolean value) {
      this.requireHttps = value;
      return this;
    }

    public Builder setAllowInsecureHttpForTests(boolean value) {
      this.allowInsecureHttpForTests = value;
      return this;
    }

    public OidcConfig build() {
      return new OidcConfig(this);
    }
  }
}
