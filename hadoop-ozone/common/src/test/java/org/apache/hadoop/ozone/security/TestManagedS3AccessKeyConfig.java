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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ManagedS3AccessKeyConfig}.
 */
public class TestManagedS3AccessKeyConfig {

  @Test
  public void defaultsMatchDesign() {
    ManagedS3AccessKeyConfig config =
        ManagedS3AccessKeyConfig.from(emptyConfiguration());

    assertThat(config.isEnabled()).isFalse();
    assertThat(config.getDefaultLifetime()).isEqualTo(Duration.ofDays(90));
    assertThat(config.getMaxLifetime()).isEqualTo(Duration.ofDays(365));
    assertThat(config.isAllowCustomSecret()).isFalse();
    assertThat(config.getSecretMinLength()).isEqualTo(32);
    assertThat(config.getEncryptionKeyName()).isEmpty();
    assertThat(config.getRetrievalHandleTtl()).isEqualTo(Duration.ofSeconds(60));
    assertThat(config.getRetrievalHandleMaxEntries()).isEqualTo(1024);
    assertThat(config.isInsecureClusterAdminAllowed()).isFalse();
    assertThat(config.isLocalPolicyEnabled()).isFalse();
    assertThat(config.getLocalPolicyMaxSizeBytes()).isEqualTo(128 * 1024);
    assertThat(config.getLocalPolicyMaxStatements()).isEqualTo(50);
    assertThat(config.getLocalPolicyMaxActionsPerStatement()).isEqualTo(100);
    assertThat(config.getLocalPolicyMaxResourcesPerStatement()).isEqualTo(100);
  }

  @Test
  public void xmlDefaultsMatchConstants() {
    OzoneConfiguration conf = new OzoneConfiguration();

    assertThat(conf.get(OZONE_S3_ACCESS_KEY_ENABLED))
        .isEqualTo(Boolean.toString(OZONE_S3_ACCESS_KEY_ENABLED_DEFAULT));
    assertThat(conf.get(OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME))
        .isEqualTo(OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME_DEFAULT);
    assertThat(conf.get(OZONE_S3_ACCESS_KEY_MAX_LIFETIME))
        .isEqualTo(OZONE_S3_ACCESS_KEY_MAX_LIFETIME_DEFAULT);
    assertThat(conf.get(OZONE_S3_ACCESS_KEY_ALLOW_CUSTOM_SECRET))
        .isEqualTo(Boolean.toString(
            OZONE_S3_ACCESS_KEY_ALLOW_CUSTOM_SECRET_DEFAULT));
    assertThat(conf.get(OZONE_S3_ACCESS_KEY_SECRET_MIN_LENGTH))
        .isEqualTo(Integer.toString(
            OZONE_S3_ACCESS_KEY_SECRET_MIN_LENGTH_DEFAULT));
    assertThat(conf.getTrimmed(OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME,
        OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME_DEFAULT))
        .isEqualTo(OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME_DEFAULT);
    assertThat(conf.get(OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL))
        .isEqualTo(OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL_DEFAULT);
    assertThat(conf.get(OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_MAX_ENTRIES))
        .isEqualTo(Integer.toString(
            OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_MAX_ENTRIES_DEFAULT));
    assertThat(conf.get(OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED))
        .isEqualTo(Boolean.toString(
            OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED_DEFAULT));
    assertThat(conf.get(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED))
        .isEqualTo(Boolean.toString(
            OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED_DEFAULT));
    assertThat(conf.get(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_SIZE))
        .isEqualTo(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_SIZE_DEFAULT);
    assertThat(conf.get(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_STATEMENTS))
        .isEqualTo(Integer.toString(
            OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_STATEMENTS_DEFAULT));
    assertThat(conf.get(
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_ACTIONS_PER_STATEMENT))
        .isEqualTo(Integer.toString(
            OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_ACTIONS_PER_STATEMENT_DEFAULT));
    assertThat(conf.get(
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_RESOURCES_PER_STATEMENT))
        .isEqualTo(Integer.toString(
            OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_RESOURCES_PER_STATEMENT_DEFAULT));
  }

  @Test
  public void parsesOverrides() {
    OzoneConfiguration conf = emptyConfiguration();
    conf.setBoolean(OZONE_S3_ACCESS_KEY_ENABLED, true);
    conf.set(OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME, "30d");
    conf.set(OZONE_S3_ACCESS_KEY_MAX_LIFETIME, "180d");
    conf.setBoolean(OZONE_S3_ACCESS_KEY_ALLOW_CUSTOM_SECRET, true);
    conf.setInt(OZONE_S3_ACCESS_KEY_SECRET_MIN_LENGTH, 64);
    conf.set(OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME, "ozone-s3-key");
    conf.set(OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL, "120s");
    conf.setInt(OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_MAX_ENTRIES, 128);
    conf.setBoolean(OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED, true);
    conf.setBoolean(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED, true);
    conf.set(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_SIZE, "64KB");
    conf.setInt(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_STATEMENTS, 10);
    conf.setInt(
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_ACTIONS_PER_STATEMENT, 20);
    conf.setInt(
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_RESOURCES_PER_STATEMENT, 30);

    ManagedS3AccessKeyConfig config = ManagedS3AccessKeyConfig.from(conf);

    assertThat(config.isEnabled()).isTrue();
    assertThat(config.getDefaultLifetime()).isEqualTo(Duration.ofDays(30));
    assertThat(config.getMaxLifetime()).isEqualTo(Duration.ofDays(180));
    assertThat(config.isAllowCustomSecret()).isTrue();
    assertThat(config.getSecretMinLength()).isEqualTo(64);
    assertThat(config.getEncryptionKeyName()).isEqualTo("ozone-s3-key");
    assertThat(config.getRetrievalHandleTtl()).isEqualTo(Duration.ofSeconds(120));
    assertThat(config.getRetrievalHandleMaxEntries()).isEqualTo(128);
    assertThat(config.isInsecureClusterAdminAllowed()).isTrue();
    assertThat(config.isLocalPolicyEnabled()).isTrue();
    assertThat(config.getLocalPolicyMaxSizeBytes()).isEqualTo(64 * 1024);
    assertThat(config.getLocalPolicyMaxStatements()).isEqualTo(10);
    assertThat(config.getLocalPolicyMaxActionsPerStatement()).isEqualTo(20);
    assertThat(config.getLocalPolicyMaxResourcesPerStatement()).isEqualTo(30);
  }

  @Test
  public void invalidScalarValuesThrowWithRelevantKey() {
    assertInvalid(OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME, "0d",
        OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME);
    assertInvalid(OZONE_S3_ACCESS_KEY_MAX_LIFETIME, "0d",
        OZONE_S3_ACCESS_KEY_MAX_LIFETIME);
    assertInvalid(OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME, "not-a-duration",
        OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME);
    assertInvalid(OZONE_S3_ACCESS_KEY_SECRET_MIN_LENGTH, "0",
        OZONE_S3_ACCESS_KEY_SECRET_MIN_LENGTH);
    assertInvalid(OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL, "0s",
        OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL);
    assertInvalid(OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL, "-1s",
        OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL);
    assertInvalid(OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL, "301s",
        OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL);
    assertInvalid(OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_MAX_ENTRIES, "0",
        OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_MAX_ENTRIES);
    assertInvalid(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_SIZE, "0B",
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_SIZE);
    assertInvalid(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_SIZE, "3GB",
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_SIZE);
    assertInvalid(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_STATEMENTS, "0",
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_STATEMENTS);
    assertInvalid(
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_ACTIONS_PER_STATEMENT, "0",
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_ACTIONS_PER_STATEMENT);
    assertInvalid(
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_RESOURCES_PER_STATEMENT, "0",
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_RESOURCES_PER_STATEMENT);

    OzoneConfiguration conf = emptyConfiguration();
    conf.set(OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME, "366d");
    conf.set(OZONE_S3_ACCESS_KEY_MAX_LIFETIME, "365d");

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> ManagedS3AccessKeyConfig.from(conf));
    assertThat(exception).hasMessageContaining(
        OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME);
    assertThat(exception).hasMessageContaining(OZONE_S3_ACCESS_KEY_MAX_LIFETIME);
  }

  @Test
  public void disabledFeatureStillValidatesScalarBounds() {
    OzoneConfiguration conf = emptyConfiguration();
    conf.setBoolean(OZONE_S3_ACCESS_KEY_ENABLED, false);
    conf.setInt(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_STATEMENTS, 0);

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> ManagedS3AccessKeyConfig.from(conf));

    assertThat(exception).hasMessageContaining(
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_MAX_STATEMENTS);
  }

  @Test
  public void disabledFeatureAcceptsEmptyEncryptionKeyName() {
    OzoneConfiguration conf = emptyConfiguration();
    conf.setBoolean(OZONE_S3_ACCESS_KEY_ENABLED, false);

    assertThat(ManagedS3AccessKeyConfig.from(conf).getEncryptionKeyName())
        .isEmpty();
  }

  @Test
  public void enabledFeatureRejectsEmptyEncryptionKeyName() {
    OzoneConfiguration conf = emptyConfiguration();
    conf.setBoolean(OZONE_S3_ACCESS_KEY_ENABLED, true);

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> ManagedS3AccessKeyConfig.from(conf));

    assertThat(exception)
        .hasMessageContaining(OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME);
    assertThat(exception).hasMessageContaining(OZONE_S3_ACCESS_KEY_ENABLED);
  }

  @Test
  public void buildValidatesScalarBounds() {
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> ManagedS3AccessKeyConfig.newBuilder()
            .setEnabled(false)
            .setSecretMinLength(0)
            .build());

    assertThat(exception).hasMessageContaining(
        OZONE_S3_ACCESS_KEY_SECRET_MIN_LENGTH);

    exception = assertThrows(
        IllegalArgumentException.class,
        () -> ManagedS3AccessKeyConfig.newBuilder()
            .setRetrievalHandleTtl(Duration.ofSeconds(301))
            .build());
    assertThat(exception).hasMessageContaining(
        OZONE_S3_ACCESS_KEY_RETRIEVAL_HANDLE_TTL);
  }

  private static void assertInvalid(String key, String value,
      String expectedMessage) {
    OzoneConfiguration conf = emptyConfiguration();
    conf.set(key, value);

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> ManagedS3AccessKeyConfig.from(conf));

    assertThat(exception).hasMessageContaining(expectedMessage);
  }

  private static OzoneConfiguration emptyConfiguration() {
    return new OzoneConfiguration(new Configuration(false));
  }
}
