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

import static org.apache.hadoop.hdds.conf.ConfigTag.OM;
import static org.apache.hadoop.hdds.conf.ConfigTag.SECURITY;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.hdds.conf.StorageUnit;

/**
 * Configuration for the experimental OIDC identity provider.
 */
@ConfigGroup(prefix = OidcConfig.PREFIX)
public final class OidcConfig {

  static final String PREFIX = "ozone.sts.web.identity";

  public static final String OZONE_STS_WEB_IDENTITY_ENABLED =
      PREFIX + ".enabled";
  public static final String OZONE_STS_WEB_IDENTITY_ISSUER_URI =
      PREFIX + ".issuer.uri";
  public static final String OZONE_STS_WEB_IDENTITY_JWKS_URI =
      PREFIX + ".jwks.uri";
  public static final String OZONE_STS_WEB_IDENTITY_AUDIENCE =
      PREFIX + ".audience";
  public static final String OZONE_STS_WEB_IDENTITY_USERNAME_CLAIM =
      PREFIX + ".username.claim";
  public static final String OZONE_STS_WEB_IDENTITY_SUBJECT_CLAIM =
      PREFIX + ".subject.claim";
  public static final String OZONE_STS_WEB_IDENTITY_GROUPS_CLAIM =
      PREFIX + ".groups.claim";
  public static final String OZONE_STS_WEB_IDENTITY_ROLES_CLAIM =
      PREFIX + ".roles.claim";
  public static final String OZONE_STS_WEB_IDENTITY_CLOCK_SKEW =
      PREFIX + ".clock.skew";
  public static final String OZONE_STS_WEB_IDENTITY_JWKS_REFRESH_INTERVAL =
      PREFIX + ".jwks.refresh.interval";
  public static final String OZONE_STS_WEB_IDENTITY_JWKS_CONNECT_TIMEOUT =
      PREFIX + ".jwks.connect.timeout";
  public static final String OZONE_STS_WEB_IDENTITY_JWKS_READ_TIMEOUT =
      PREFIX + ".jwks.read.timeout";
  public static final String OZONE_STS_WEB_IDENTITY_JWKS_SIZE_LIMIT =
      PREFIX + ".jwks.size.limit";
  public static final String OZONE_STS_WEB_IDENTITY_REQUIRE_HTTPS =
      PREFIX + ".require.https";

  private static final String ENABLED_DEFAULT = "false";
  private static final String ISSUER_URI_DEFAULT = "";
  private static final String JWKS_URI_DEFAULT = "";
  private static final String AUDIENCE_DEFAULT = "";
  private static final String USERNAME_CLAIM_DEFAULT = "preferred_username";
  private static final String SUBJECT_CLAIM_DEFAULT = "sub";
  private static final String GROUPS_CLAIM_DEFAULT = "groups";
  private static final String ROLES_CLAIM_DEFAULT = "realm_access.roles";
  private static final String CLOCK_SKEW_DEFAULT = "60s";
  private static final String JWKS_REFRESH_INTERVAL_DEFAULT = "10m";
  private static final String JWKS_CONNECT_TIMEOUT_DEFAULT = "5s";
  private static final String JWKS_READ_TIMEOUT_DEFAULT = "5s";
  private static final String JWKS_SIZE_LIMIT_DEFAULT = "1MB";
  private static final String REQUIRE_HTTPS_DEFAULT = "true";

  @Config(key = OZONE_STS_WEB_IDENTITY_ENABLED,
      defaultValue = ENABLED_DEFAULT,
      type = ConfigType.BOOLEAN,
      description = "Enables experimental Ozone STS "
          + "AssumeRoleWithWebIdentity support.",
      tags = {OM, SECURITY})
  private boolean enabled;

  @Config(key = OZONE_STS_WEB_IDENTITY_ISSUER_URI,
      defaultValue = ISSUER_URI_DEFAULT,
      type = ConfigType.STRING,
      description = "Expected OIDC issuer URI. Required when "
          + "AssumeRoleWithWebIdentity support is enabled.",
      tags = {OM, SECURITY})
  private String issuerUri;

  @Config(key = OZONE_STS_WEB_IDENTITY_JWKS_URI,
      defaultValue = JWKS_URI_DEFAULT,
      type = ConfigType.STRING,
      description = "Optional JWKS endpoint URI. If unset, OM resolves JWKS "
          + "from the issuer discovery metadata.",
      tags = {OM, SECURITY})
  private String jwksUri;

  @Config(key = OZONE_STS_WEB_IDENTITY_AUDIENCE,
      defaultValue = AUDIENCE_DEFAULT,
      type = ConfigType.STRING,
      description = "Expected OIDC audience value for WebIdentity tokens.",
      tags = {OM, SECURITY})
  private String audience;

  @Config(key = OZONE_STS_WEB_IDENTITY_USERNAME_CLAIM,
      defaultValue = USERNAME_CLAIM_DEFAULT,
      type = ConfigType.STRING,
      description = "OIDC claim used as the Ozone user name.",
      tags = {OM, SECURITY})
  private String usernameClaim = USERNAME_CLAIM_DEFAULT;

  @Config(key = OZONE_STS_WEB_IDENTITY_SUBJECT_CLAIM,
      defaultValue = SUBJECT_CLAIM_DEFAULT,
      type = ConfigType.STRING,
      description = "OIDC claim used as the stable token subject.",
      tags = {OM, SECURITY})
  private String subjectClaim = SUBJECT_CLAIM_DEFAULT;

  @Config(key = OZONE_STS_WEB_IDENTITY_GROUPS_CLAIM,
      defaultValue = GROUPS_CLAIM_DEFAULT,
      type = ConfigType.STRING,
      description = "OIDC claim used to import group attributes.",
      tags = {OM, SECURITY})
  private String groupsClaim = GROUPS_CLAIM_DEFAULT;

  @Config(key = OZONE_STS_WEB_IDENTITY_ROLES_CLAIM,
      defaultValue = ROLES_CLAIM_DEFAULT,
      type = ConfigType.STRING,
      description = "OIDC claim path used to import role attributes.",
      tags = {OM, SECURITY})
  private String rolesClaim = ROLES_CLAIM_DEFAULT;

  @Config(key = OZONE_STS_WEB_IDENTITY_CLOCK_SKEW,
      defaultValue = CLOCK_SKEW_DEFAULT,
      type = ConfigType.TIME,
      timeUnit = TimeUnit.MILLISECONDS,
      description = "Allowed clock skew while validating OIDC temporal "
          + "claims.",
      tags = {OM, SECURITY})
  private Duration clockSkew = Duration.ofSeconds(60);

  @Config(key = OZONE_STS_WEB_IDENTITY_JWKS_REFRESH_INTERVAL,
      defaultValue = JWKS_REFRESH_INTERVAL_DEFAULT,
      type = ConfigType.TIME,
      timeUnit = TimeUnit.MILLISECONDS,
      description = "Interval after which cached OIDC JWKS keys are "
          + "refreshed.",
      tags = {OM, SECURITY})
  private Duration jwksRefreshInterval = Duration.ofMinutes(10);

  @Config(key = OZONE_STS_WEB_IDENTITY_JWKS_CONNECT_TIMEOUT,
      defaultValue = JWKS_CONNECT_TIMEOUT_DEFAULT,
      type = ConfigType.TIME,
      timeUnit = TimeUnit.MILLISECONDS,
      description = "Connect timeout for loading OIDC JWKS keys.",
      tags = {OM, SECURITY})
  private Duration jwksConnectTimeout = Duration.ofSeconds(5);

  @Config(key = OZONE_STS_WEB_IDENTITY_JWKS_READ_TIMEOUT,
      defaultValue = JWKS_READ_TIMEOUT_DEFAULT,
      type = ConfigType.TIME,
      timeUnit = TimeUnit.MILLISECONDS,
      description = "Read timeout for loading OIDC JWKS keys.",
      tags = {OM, SECURITY})
  private Duration jwksReadTimeout = Duration.ofSeconds(5);

  @Config(key = OZONE_STS_WEB_IDENTITY_JWKS_SIZE_LIMIT,
      defaultValue = JWKS_SIZE_LIMIT_DEFAULT,
      type = ConfigType.SIZE,
      sizeUnit = StorageUnit.BYTES,
      description = "Maximum accepted OIDC JWKS response size.",
      tags = {OM, SECURITY})
  private int jwksSizeLimit = 1024 * 1024;

  @Config(key = OZONE_STS_WEB_IDENTITY_REQUIRE_HTTPS,
      defaultValue = REQUIRE_HTTPS_DEFAULT,
      type = ConfigType.BOOLEAN,
      description = "Requires HTTPS issuer and JWKS URIs for WebIdentity "
          + "token validation. Tests and local development may set this to "
          + "false explicitly.",
      tags = {OM, SECURITY})
  private boolean requireHttps = true;

  public OidcConfig() {
  }

  private OidcConfig(Builder builder) {
    this.enabled = builder.enabled;
    this.issuerUri = builder.issuerUri;
    this.jwksUri = builder.jwksUri;
    this.audience = builder.audience;
    this.usernameClaim = builder.usernameClaim;
    this.subjectClaim = builder.subjectClaim;
    this.groupsClaim = builder.groupsClaim;
    this.rolesClaim = builder.rolesClaim;
    this.clockSkew = builder.clockSkew;
    this.jwksRefreshInterval = builder.jwksRefreshInterval;
    this.jwksConnectTimeout = builder.jwksConnectTimeout;
    this.jwksReadTimeout = builder.jwksReadTimeout;
    this.jwksSizeLimit = builder.jwksSizeLimit;
    this.requireHttps = builder.requireHttps;
    normalizeAndValidate();
  }

  public static OidcConfig from(ConfigurationSource conf) {
    return conf.getObject(OidcConfig.class);
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

  @PostConstruct
  public void normalizeAndValidate() {
    issuerUri = StringUtils.trimToEmpty(issuerUri);
    jwksUri = StringUtils.trimToEmpty(jwksUri);
    audience = StringUtils.trimToEmpty(audience);
    usernameClaim = requireNonBlank(usernameClaim,
        OZONE_STS_WEB_IDENTITY_USERNAME_CLAIM);
    subjectClaim = requireNonBlank(subjectClaim,
        OZONE_STS_WEB_IDENTITY_SUBJECT_CLAIM);
    groupsClaim = requireNonBlank(groupsClaim,
        OZONE_STS_WEB_IDENTITY_GROUPS_CLAIM);
    rolesClaim = requireNonBlank(rolesClaim,
        OZONE_STS_WEB_IDENTITY_ROLES_CLAIM);
    clockSkew = requireNonNegative(clockSkew,
        OZONE_STS_WEB_IDENTITY_CLOCK_SKEW);
    jwksRefreshInterval = requireNonNegative(jwksRefreshInterval,
        OZONE_STS_WEB_IDENTITY_JWKS_REFRESH_INTERVAL);
    jwksConnectTimeout = requirePositive(jwksConnectTimeout,
        OZONE_STS_WEB_IDENTITY_JWKS_CONNECT_TIMEOUT);
    jwksReadTimeout = requirePositive(jwksReadTimeout,
        OZONE_STS_WEB_IDENTITY_JWKS_READ_TIMEOUT);
    jwksSizeLimit = requirePositive(jwksSizeLimit,
        OZONE_STS_WEB_IDENTITY_JWKS_SIZE_LIMIT);

    if (enabled) {
      validateForProvider();
    }
  }

  void validateForProvider() {
    requireNonBlank(issuerUri, OZONE_STS_WEB_IDENTITY_ISSUER_URI);
    requireNonBlank(audience, OZONE_STS_WEB_IDENTITY_AUDIENCE);

    if (requireHttps) {
      requireHttpsUri(issuerUri, OZONE_STS_WEB_IDENTITY_ISSUER_URI);
      if (StringUtils.isNotEmpty(jwksUri)) {
        requireHttpsUri(jwksUri, OZONE_STS_WEB_IDENTITY_JWKS_URI);
      }
    }
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
    String trimmed = StringUtils.trimToEmpty(value);
    if (StringUtils.isBlank(trimmed)) {
      throw new IllegalArgumentException(key + " must not be empty");
    }
    return trimmed;
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

    private boolean enabled;
    private String issuerUri = ISSUER_URI_DEFAULT;
    private String jwksUri = JWKS_URI_DEFAULT;
    private String audience = AUDIENCE_DEFAULT;
    private String usernameClaim = USERNAME_CLAIM_DEFAULT;
    private String subjectClaim = SUBJECT_CLAIM_DEFAULT;
    private String groupsClaim = GROUPS_CLAIM_DEFAULT;
    private String rolesClaim = ROLES_CLAIM_DEFAULT;
    private Duration clockSkew = Duration.ofSeconds(60);
    private Duration jwksRefreshInterval = Duration.ofMinutes(10);
    private Duration jwksConnectTimeout = Duration.ofSeconds(5);
    private Duration jwksReadTimeout = Duration.ofSeconds(5);
    private int jwksSizeLimit = 1024 * 1024;
    private boolean requireHttps = true;

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

    public OidcConfig build() {
      return new OidcConfig(this);
    }
  }
}
