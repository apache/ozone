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

package org.apache.hadoop.ozone.security;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_ALLOW_CUSTOM_SECRET;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_ALLOW_CUSTOM_SECRET_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_ACTIONS_PER_STATEMENT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_ACTIONS_PER_STATEMENT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_RESOURCES_PER_STATEMENT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_RESOURCES_PER_STATEMENT_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_STATEMENTS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_STATEMENTS_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_MAX_LIFETIME;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_MAX_LIFETIME_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_MAX_ENTRIES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_MAX_ENTRIES_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_SECRET_MIN_LENGTH;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_SECRET_MIN_LENGTH_DEFAULT;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;

/**
 * Configuration for OM-managed local S3 access keys.
 */
public final class ManagedS3AccessKeyConfig {

  private static final Duration RETRIEVAL_HANDLE_TTL_MAX =
      Duration.ofSeconds(300);

  private final boolean enabled;
  private final Duration defaultLifetime;
  private final Duration maxLifetime;
  private final boolean allowCustomSecret;
  private final int secretMinLength;
  private final String encryptionKeyName;
  private final Duration retrievalHandleTtl;
  private final int retrievalHandleMaxEntries;
  private final boolean insecureClusterAdminAllowed;
  private final boolean localPolicyEnabled;
  private final int localPolicyMaxSizeBytes;
  private final int localPolicyMaxStatements;
  private final int localPolicyMaxActionsPerStatement;
  private final int localPolicyMaxResourcesPerStatement;

  private ManagedS3AccessKeyConfig(Builder builder) {
    this.enabled = builder.enabled;
    this.defaultLifetime = requirePositive(builder.defaultLifetime,
        OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME);
    this.maxLifetime = requirePositive(builder.maxLifetime,
        OZONE_S3_ACCESS_KEY_MAX_LIFETIME);
    requireDefaultLifetimeWithinMax(defaultLifetime, maxLifetime);
    this.allowCustomSecret = builder.allowCustomSecret;
    this.secretMinLength = requirePositive(builder.secretMinLength,
        OZONE_S3_ACCESS_KEY_SECRET_MIN_LENGTH);
    this.encryptionKeyName = normalize(builder.encryptionKeyName);
    if (enabled && encryptionKeyName.isEmpty()) {
      throw new IllegalArgumentException(
          OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME
              + " must be configured when "
              + OZONE_S3_ACCESS_KEY_ENABLED + "=true");
    }
    this.retrievalHandleTtl = requireMax(
        requirePositive(builder.retrievalHandleTtl,
            OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL),
        RETRIEVAL_HANDLE_TTL_MAX, OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL);
    this.retrievalHandleMaxEntries = requirePositive(
        builder.retrievalHandleMaxEntries,
        OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_MAX_ENTRIES);
    this.insecureClusterAdminAllowed = builder.insecureClusterAdminAllowed;
    this.localPolicyEnabled = builder.localPolicyEnabled;
    this.localPolicyMaxSizeBytes = requirePositive(
        builder.localPolicyMaxSizeBytes,
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_SIZE);
    this.localPolicyMaxStatements = requirePositive(
        builder.localPolicyMaxStatements,
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_STATEMENTS);
    this.localPolicyMaxActionsPerStatement = requirePositive(
        builder.localPolicyMaxActionsPerStatement,
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_ACTIONS_PER_STATEMENT);
    this.localPolicyMaxResourcesPerStatement = requirePositive(
        builder.localPolicyMaxResourcesPerStatement,
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_RESOURCES_PER_STATEMENT);
  }

  public static ManagedS3AccessKeyConfig from(ConfigurationSource conf) {
    return newBuilder()
        .setEnabled(conf.getBoolean(OZONE_S3_ACCESS_KEY_ENABLED,
            OZONE_S3_ACCESS_KEY_ENABLED_DEFAULT))
        .setDefaultLifetime(duration(conf,
            OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME,
            OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME_DEFAULT))
        .setMaxLifetime(duration(conf, OZONE_S3_ACCESS_KEY_MAX_LIFETIME,
            OZONE_S3_ACCESS_KEY_MAX_LIFETIME_DEFAULT))
        .setAllowCustomSecret(conf.getBoolean(
            OZONE_S3_ACCESS_KEY_ALLOW_CUSTOM_SECRET,
            OZONE_S3_ACCESS_KEY_ALLOW_CUSTOM_SECRET_DEFAULT))
        .setSecretMinLength(integer(conf,
            OZONE_S3_ACCESS_KEY_SECRET_MIN_LENGTH,
            OZONE_S3_ACCESS_KEY_SECRET_MIN_LENGTH_DEFAULT))
        .setEncryptionKeyName(conf.getTrimmed(
            OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME,
            OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME_DEFAULT))
        .setRetrievalHandleTtl(duration(conf,
            OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL,
            OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL_DEFAULT))
        .setRetrievalHandleMaxEntries(integer(conf,
            OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_MAX_ENTRIES,
            OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_MAX_ENTRIES_DEFAULT))
        .setInsecureClusterAdminAllowed(conf.getBoolean(
            OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED,
            OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED_DEFAULT))
        .setLocalPolicyEnabled(conf.getBoolean(
            OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED,
            OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED_DEFAULT))
        .setLocalPolicyMaxSizeBytes(storageSize(conf,
            OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_SIZE,
            OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_SIZE_DEFAULT))
        .setLocalPolicyMaxStatements(integer(conf,
            OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_STATEMENTS,
            OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_STATEMENTS_DEFAULT))
        .setLocalPolicyMaxActionsPerStatement(integer(conf,
            OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_ACTIONS_PER_STATEMENT,
            OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_ACTIONS_PER_STATEMENT_DEFAULT))
        .setLocalPolicyMaxResourcesPerStatement(integer(conf,
            OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_RESOURCES_PER_STATEMENT,
            OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_RESOURCES_PER_STATEMENT_DEFAULT))
        .build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public boolean isEnabled() {
    return enabled;
  }

  public Duration getDefaultLifetime() {
    return defaultLifetime;
  }

  public Duration getMaxLifetime() {
    return maxLifetime;
  }

  public boolean isAllowCustomSecret() {
    return allowCustomSecret;
  }

  public int getSecretMinLength() {
    return secretMinLength;
  }

  public String getEncryptionKeyName() {
    return encryptionKeyName;
  }

  public Duration getRetrievalHandleTtl() {
    return retrievalHandleTtl;
  }

  public int getRetrievalHandleMaxEntries() {
    return retrievalHandleMaxEntries;
  }

  public boolean isInsecureClusterAdminAllowed() {
    return insecureClusterAdminAllowed;
  }

  public boolean isLocalPolicyEnabled() {
    return localPolicyEnabled;
  }

  public int getLocalPolicyMaxSizeBytes() {
    return localPolicyMaxSizeBytes;
  }

  public int getLocalPolicyMaxStatements() {
    return localPolicyMaxStatements;
  }

  public int getLocalPolicyMaxActionsPerStatement() {
    return localPolicyMaxActionsPerStatement;
  }

  public int getLocalPolicyMaxResourcesPerStatement() {
    return localPolicyMaxResourcesPerStatement;
  }

  private static Duration duration(ConfigurationSource conf, String key,
      String defaultValue) {
    try {
      return Duration.ofMillis(conf.getTimeDuration(key, defaultValue,
          TimeUnit.MILLISECONDS));
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(key + " has invalid duration", e);
    }
  }

  private static int integer(ConfigurationSource conf, String key,
      int defaultValue) {
    try {
      return conf.getInt(key, defaultValue);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(key + " must be an integer", e);
    }
  }

  private static int storageSize(ConfigurationSource conf, String key,
      String defaultValue) {
    final double bytes;
    try {
      bytes = conf.getStorageSize(key, defaultValue, StorageUnit.BYTES);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(key + " has invalid storage size", e);
    }
    if (!Double.isFinite(bytes)) {
      throw new IllegalArgumentException(key + " must be finite");
    }
    if (bytes <= 0) {
      throw new IllegalArgumentException(key + " must be positive");
    }
    if (bytes > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(key + " must not exceed "
          + Integer.MAX_VALUE + " bytes");
    }
    return (int) bytes;
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

  private static Duration requireMax(Duration value, Duration max, String key) {
    if (value.compareTo(max) > 0) {
      throw new IllegalArgumentException(key + " must not exceed " + max);
    }
    return value;
  }

  private static void requireDefaultLifetimeWithinMax(Duration defaultLifetime,
      Duration maxLifetime) {
    if (defaultLifetime.compareTo(maxLifetime) > 0) {
      throw new IllegalArgumentException(
          OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME + " must not exceed "
              + OZONE_S3_ACCESS_KEY_MAX_LIFETIME);
    }
  }

  private static String normalize(String value) {
    return value == null ? "" : value.trim();
  }

  /**
   * Builder for {@link ManagedS3AccessKeyConfig}.
   */
  public static final class Builder {

    private boolean enabled = OZONE_S3_ACCESS_KEY_ENABLED_DEFAULT;
    private Duration defaultLifetime = Duration.ofDays(90);
    private Duration maxLifetime = Duration.ofDays(365);
    private boolean allowCustomSecret =
        OZONE_S3_ACCESS_KEY_ALLOW_CUSTOM_SECRET_DEFAULT;
    private int secretMinLength =
        OZONE_S3_ACCESS_KEY_SECRET_MIN_LENGTH_DEFAULT;
    private String encryptionKeyName =
        OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME_DEFAULT;
    private Duration retrievalHandleTtl = Duration.ofSeconds(60);
    private int retrievalHandleMaxEntries =
        OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_MAX_ENTRIES_DEFAULT;
    private boolean insecureClusterAdminAllowed =
        OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED_DEFAULT;
    private boolean localPolicyEnabled =
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED_DEFAULT;
    private int localPolicyMaxSizeBytes = 128 * 1024;
    private int localPolicyMaxStatements =
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_STATEMENTS_DEFAULT;
    private int localPolicyMaxActionsPerStatement =
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_ACTIONS_PER_STATEMENT_DEFAULT;
    private int localPolicyMaxResourcesPerStatement =
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_RESOURCES_PER_STATEMENT_DEFAULT;

    private Builder() {
    }

    public Builder setEnabled(boolean value) {
      this.enabled = value;
      return this;
    }

    public Builder setDefaultLifetime(Duration value) {
      this.defaultLifetime = value;
      return this;
    }

    public Builder setMaxLifetime(Duration value) {
      this.maxLifetime = value;
      return this;
    }

    public Builder setAllowCustomSecret(boolean value) {
      this.allowCustomSecret = value;
      return this;
    }

    public Builder setSecretMinLength(int value) {
      this.secretMinLength = value;
      return this;
    }

    public Builder setEncryptionKeyName(String value) {
      this.encryptionKeyName = value;
      return this;
    }

    public Builder setRetrievalHandleTtl(Duration value) {
      this.retrievalHandleTtl = value;
      return this;
    }

    public Builder setRetrievalHandleMaxEntries(int value) {
      this.retrievalHandleMaxEntries = value;
      return this;
    }

    public Builder setInsecureClusterAdminAllowed(boolean value) {
      this.insecureClusterAdminAllowed = value;
      return this;
    }

    public Builder setLocalPolicyEnabled(boolean value) {
      this.localPolicyEnabled = value;
      return this;
    }

    public Builder setLocalPolicyMaxSizeBytes(int value) {
      this.localPolicyMaxSizeBytes = value;
      return this;
    }

    public Builder setLocalPolicyMaxStatements(int value) {
      this.localPolicyMaxStatements = value;
      return this;
    }

    public Builder setLocalPolicyMaxActionsPerStatement(int value) {
      this.localPolicyMaxActionsPerStatement = value;
      return this;
    }

    public Builder setLocalPolicyMaxResourcesPerStatement(int value) {
      this.localPolicyMaxResourcesPerStatement = value;
      return this;
    }

    public ManagedS3AccessKeyConfig build() {
      return new ManagedS3AccessKeyConfig(this);
    }
  }
}
