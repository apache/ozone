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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_MAX_LIFETIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Tests OM startup validation for managed local S3 access keys.
 */
public class TestOzoneManagerManagedS3AccessKeyStartup {

  private static final String KEY_NAME = "ozone-s3-managed-access-keys";

  @Test
  public void disabledPassesInNonSecureCluster() {
    assertDoesNotThrow(() -> OzoneManager
        .validateManagedS3AccessKeyStartup(conf(false), false));
  }

  @Test
  public void secureEnabledPassesRegardlessOfUnsafeLocalFlags() {
    OzoneConfiguration conf = conf(true);
    conf.setBoolean(OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED, false);
    conf.setBoolean(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED, false);

    assertDoesNotThrow(() -> OzoneManager
        .validateManagedS3AccessKeyStartup(conf, true));
  }

  @Test
  public void enabledRejectsEmptyEncryptionKeyName() {
    OzoneConfiguration conf =
        new OzoneConfiguration(new Configuration(false));
    conf.setBoolean(OZONE_S3_ACCESS_KEY_ENABLED, true);

    ConfigurationException exception = assertThrows(
        ConfigurationException.class,
        () -> OzoneManager.validateManagedS3AccessKeyStartup(conf, true));

    assertThat(exception).hasCauseInstanceOf(IllegalArgumentException.class);
    assertThat(exception.getCause())
        .hasMessageContaining(OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME);
  }

  @Test
  public void nonSecureEnabledRejectsMissingAdminGate() {
    OzoneConfiguration conf = conf(true);
    conf.setBoolean(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED, true);

    ConfigurationException exception = assertThrows(
        ConfigurationException.class,
        () -> OzoneManager.validateManagedS3AccessKeyStartup(conf, false));

    assertThat(exception).hasMessageContaining(
        OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED);
  }

  @Test
  public void nonSecureEnabledRejectsMissingLocalPolicyEvenWhenAdminAllowed() {
    OzoneConfiguration conf = conf(true);
    conf.setBoolean(OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED, true);
    conf.setBoolean(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED, false);

    ConfigurationException exception = assertThrows(
        ConfigurationException.class,
        () -> OzoneManager.validateManagedS3AccessKeyStartup(conf, false));

    assertThat(exception).hasMessageContaining(
        OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED);
    assertThat(exception).hasMessageContaining(
        OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED);
    assertThat(exception).hasMessageContaining("does not bypass");
  }

  @Test
  public void nonSecureEnabledAcceptsOnlyWhenBothGatesAreExplicit() {
    OzoneConfiguration conf = conf(true);
    conf.setBoolean(OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED, true);
    conf.setBoolean(OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED, true);

    assertDoesNotThrow(() -> OzoneManager
        .validateManagedS3AccessKeyStartup(conf, false));
  }

  @Test
  public void disabledKmsValidationPreservesMissingProviderBehavior() {
    assertDoesNotThrow(() -> OzoneManager
        .validateManagedS3AccessKeyKmsStartup(conf(false), null));
  }

  @Test
  public void enabledKmsValidationRejectsMissingProviderPath() {
    ConfigurationException exception = assertThrows(
        ConfigurationException.class,
        () -> OzoneManager.validateManagedS3AccessKeyKmsStartup(conf(true)));

    assertThat(exception).hasMessageContaining(HADOOP_SECURITY_KEY_PROVIDER_PATH);
  }

  @Test
  public void enabledKmsValidationRejectsMissingConfiguredKey()
      throws IOException {
    KeyProviderCryptoExtension provider = provider(false, false);

    ConfigurationException exception = assertThrows(
        ConfigurationException.class,
        () -> OzoneManager.validateManagedS3AccessKeyKmsStartup(
            conf(true), provider));

    assertThat(exception)
        .hasMessageContaining(OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME);
    assertThat(exception).hasMessageContaining(KEY_NAME);
    assertThat(exception).hasMessageContaining("does not exist");
  }

  @Test
  public void enabledKmsValidationRejectsTransientProvider()
      throws IOException {
    KeyProviderCryptoExtension provider = provider(true, true);
    when(provider.isTransient()).thenReturn(true);

    ConfigurationException exception = assertThrows(
        ConfigurationException.class,
        () -> OzoneManager.validateManagedS3AccessKeyKmsStartup(
            conf(true), provider));

    assertThat(exception).hasMessageContaining("durable");
    assertThat(exception).hasMessageContaining(HADOOP_SECURITY_KEY_PROVIDER_PATH);
  }

  @Test
  public void enabledKmsValidationRejectsMissingCurrentKeyVersion()
      throws IOException {
    KeyProviderCryptoExtension provider = provider(true, false);

    ConfigurationException exception = assertThrows(
        ConfigurationException.class,
        () -> OzoneManager.validateManagedS3AccessKeyKmsStartup(
            conf(true), provider));

    assertThat(exception).hasMessageContaining(KEY_NAME);
    assertThat(exception).hasMessageContaining("current key version");
  }

  @Test
  public void enabledKmsValidationAcceptsConfiguredCurrentKey()
      throws IOException {
    assertDoesNotThrow(() -> OzoneManager
        .validateManagedS3AccessKeyKmsStartup(conf(true),
            provider(true, true)));
  }

  @Test
  public void invalidHelperConfigWrapsAsConfigurationException() {
    OzoneConfiguration conf = conf(false);
    conf.set(OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME, "366d");
    conf.set(OZONE_S3_ACCESS_KEY_MAX_LIFETIME, "365d");

    ConfigurationException exception = assertThrows(
        ConfigurationException.class,
        () -> OzoneManager.validateManagedS3AccessKeyStartup(conf, false));

    assertThat(exception)
        .hasMessage("Invalid managed S3 access key configuration");
    assertThat(exception).hasCauseInstanceOf(IllegalArgumentException.class);
    assertThat(exception.getCause()).hasMessageContaining(
        OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME);
  }

  private static OzoneConfiguration conf(boolean enabled) {
    OzoneConfiguration conf =
        new OzoneConfiguration(new Configuration(false));
    conf.setBoolean(OZONE_S3_ACCESS_KEY_ENABLED, enabled);
    if (enabled) {
      conf.set(OZONE_S3_ACCESS_KEY_ENCRYPTION_KEY_NAME, KEY_NAME);
    }
    return conf;
  }

  private static KeyProviderCryptoExtension provider(boolean hasKey,
      boolean hasCurrentKeyVersion) throws IOException {
    KeyProviderCryptoExtension provider =
        mock(KeyProviderCryptoExtension.class);
    if (hasKey) {
      when(provider.getMetadata(KEY_NAME))
          .thenReturn(mock(KeyProvider.Metadata.class));
    }
    if (hasCurrentKeyVersion) {
      when(provider.getCurrentKey(KEY_NAME))
          .thenReturn(mock(KeyProvider.KeyVersion.class));
    }
    return provider;
  }
}
