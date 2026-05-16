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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_DEFAULT_LIFETIME;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_INSECURE_CLUSTER_ADMIN_ALLOWED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_LOCAL_POLICY_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_S3_ACCESS_KEY_MAX_LIFETIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;

/**
 * Tests OM startup validation for managed local S3 access keys.
 */
public class TestOzoneManagerManagedS3AccessKeyStartup {

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
    return conf;
  }
}
