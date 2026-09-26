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

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NODES_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_MAX_RETRY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_RETRY_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT_RETRIES;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_MAX_RETRY_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_RPC_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import javax.net.SocketFactory;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ClientCache;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.junit.jupiter.api.Test;

/** Tests OM-specific SCM client configuration. */
class TestOMScmLocationClientConfig {
  @Test
  void overridesScmClientAndIpcTimeoutsWithoutMutatingSourceConfiguration() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.setTimeDuration(OzoneConfigKeys.HDDS_SCM_CLIENT_RPC_TIME_OUT,
        15, TimeUnit.MINUTES);
    configuration.setTimeDuration(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
        20, TimeUnit.SECONDS);
    configuration.setInt(
        CommonConfigurationKeysPublic
            .IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        45);
    configuration.setInt(OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_MAX_RETRY,
        15);
    configuration.setTimeDuration(
        OzoneConfigKeys.HDDS_SCM_CLIENT_MAX_RETRY_TIMEOUT,
        10, TimeUnit.MINUTES);
    configuration.setTimeDuration(
        OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_RETRY_INTERVAL,
        9, TimeUnit.SECONDS);
    configuration.setTimeDuration(OZONE_OM_SCM_LOCATION_CLIENT_RPC_TIMEOUT,
        60, TimeUnit.SECONDS);
    configuration.setTimeDuration(
        OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT,
        7, TimeUnit.SECONDS);
    configuration.setInt(
        OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT_RETRIES, 1);
    configuration.setInt(OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_MAX_RETRY, 4);
    configuration.setTimeDuration(
        OZONE_OM_SCM_LOCATION_CLIENT_MAX_RETRY_TIMEOUT,
        8, TimeUnit.SECONDS);
    configuration.setTimeDuration(
        OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_RETRY_INTERVAL,
        1, TimeUnit.SECONDS);

    OzoneConfiguration scmClientConfiguration =
        OMScmLocationClientConfig.createScmClientConfiguration(configuration);

    assertEquals(60, scmClientConfiguration.getTimeDuration(
        OzoneConfigKeys.HDDS_SCM_CLIENT_RPC_TIME_OUT, 0, TimeUnit.SECONDS));
    assertEquals(7_000, scmClientConfiguration.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY, 0));
    assertEquals(1, scmClientConfiguration.getInt(
        CommonConfigurationKeysPublic
            .IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        0));
    assertEquals(4, scmClientConfiguration.getInt(
        OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_MAX_RETRY, 0));
    assertEquals(8, scmClientConfiguration.getTimeDuration(
        OzoneConfigKeys.HDDS_SCM_CLIENT_MAX_RETRY_TIMEOUT,
        0, TimeUnit.SECONDS));
    assertEquals(1, scmClientConfiguration.getTimeDuration(
        OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_RETRY_INTERVAL,
        0, TimeUnit.SECONDS));
    assertEquals(15, configuration.getTimeDuration(
        OzoneConfigKeys.HDDS_SCM_CLIENT_RPC_TIME_OUT, 0, TimeUnit.MINUTES));
    assertEquals(20, configuration.getTimeDuration(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
        0, TimeUnit.SECONDS));
    assertEquals(45, configuration.getInt(
        CommonConfigurationKeysPublic
            .IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        0));
    assertEquals(15, configuration.getInt(
        OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_MAX_RETRY, 0));
    assertEquals(10, configuration.getTimeDuration(
        OzoneConfigKeys.HDDS_SCM_CLIENT_MAX_RETRY_TIMEOUT,
        0, TimeUnit.MINUTES));
    assertEquals(9, configuration.getTimeDuration(
        OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_RETRY_INTERVAL,
        0, TimeUnit.SECONDS));
  }

  @Test
  void appliesOmDefaultsWhenOverridesAreNotSet() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.setTimeDuration(OzoneConfigKeys.HDDS_SCM_CLIENT_RPC_TIME_OUT,
        2, TimeUnit.MINUTES);
    configuration.setTimeDuration(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
        10, TimeUnit.SECONDS);
    configuration.setInt(
        CommonConfigurationKeysPublic
            .IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        3);
    configuration.setInt(OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_MAX_RETRY,
        15);
    configuration.setTimeDuration(
        OzoneConfigKeys.HDDS_SCM_CLIENT_MAX_RETRY_TIMEOUT,
        10, TimeUnit.MINUTES);
    configuration.setTimeDuration(
        OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_RETRY_INTERVAL,
        9, TimeUnit.SECONDS);

    OzoneConfiguration scmClientConfiguration =
        OMScmLocationClientConfig.createScmClientConfiguration(configuration);

    assertEquals(30, scmClientConfiguration.getTimeDuration(
        OzoneConfigKeys.HDDS_SCM_CLIENT_RPC_TIME_OUT, 0, TimeUnit.SECONDS));
    assertEquals(5_000, scmClientConfiguration.getInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY, 0));
    assertEquals(0, scmClientConfiguration.getInt(
        CommonConfigurationKeysPublic
            .IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        0));
    assertEquals(3, scmClientConfiguration.getInt(
        OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_MAX_RETRY, 0));
    assertEquals(6, scmClientConfiguration.getTimeDuration(
        OzoneConfigKeys.HDDS_SCM_CLIENT_MAX_RETRY_TIMEOUT,
        0, TimeUnit.SECONDS));
    assertEquals(2, scmClientConfiguration.getTimeDuration(
        OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_RETRY_INTERVAL,
        0, TimeUnit.SECONDS));
  }

  @Test
  void boundsRetryCountByConfiguredScmNodes() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    String scmServiceId = "scmservice";
    configuration.set(OZONE_SCM_SERVICE_IDS_KEY, scmServiceId);
    configuration.set(OZONE_SCM_NODES_KEY + "." + scmServiceId,
        "scm1,scm2,scm3,scm4,scm5,scm6");
    configuration.setInt(OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_MAX_RETRY, 3);

    OzoneConfiguration scmClientConfiguration =
        OMScmLocationClientConfig.createScmClientConfiguration(configuration);

    assertEquals(6, scmClientConfiguration.getInt(
        OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_MAX_RETRY, 0));
    assertEquals(3, configuration.getInt(
        OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_MAX_RETRY, 0));

    configuration.setInt(OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_MAX_RETRY, 8);
    scmClientConfiguration =
        OMScmLocationClientConfig.createScmClientConfiguration(configuration);
    assertEquals(8, scmClientConfiguration.getInt(
        OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_MAX_RETRY, 0));
  }

  @Test
  void rejectsNonPositiveFailoverRetryInterval() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_RETRY_INTERVAL,
        "0ms");

    ConfigurationException zeroInterval = assertThrows(
        ConfigurationException.class,
        () -> OMScmLocationClientConfig.createScmClientConfiguration(
            configuration));
    assertTrue(zeroInterval.getMessage().contains(
        OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_RETRY_INTERVAL));

    configuration.set(OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_RETRY_INTERVAL,
        "500us");
    ConfigurationException subMillisecondInterval = assertThrows(
        ConfigurationException.class,
        () -> OMScmLocationClientConfig.createScmClientConfiguration(
            configuration));
    assertTrue(subMillisecondInterval.getMessage().contains(
        OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_RETRY_INTERVAL));
  }

  @Test
  void rejectsConnectTimeoutOutsideHadoopIpcIntegerRange() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.setTimeDuration(
        OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT,
        -1, TimeUnit.MILLISECONDS);

    ConfigurationException negativeTimeout = assertThrows(
        ConfigurationException.class,
        () -> OMScmLocationClientConfig.createScmClientConfiguration(
            configuration));
    assertTrue(negativeTimeout.getMessage().contains(
        OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT));

    configuration.setTimeDuration(
        OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT,
        (long) Integer.MAX_VALUE + 1, TimeUnit.MILLISECONDS);
    ConfigurationException excessiveTimeout = assertThrows(
        ConfigurationException.class,
        () -> OMScmLocationClientConfig.createScmClientConfiguration(
            configuration));
    assertTrue(excessiveTimeout.getMessage().contains(
        OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT));
  }

  @Test
  void usesAnIpcClientSeparateFromTheDefaultSocketFactory() {
    OzoneConfiguration configuration = new OzoneConfiguration();
    SocketFactory defaultSocketFactory =
        NetUtils.getDefaultSocketFactory(configuration);
    SocketFactory scmLocationSocketFactory =
        OMScmLocationClientConfig.createSocketFactory(configuration);
    ClientCache clientCache = new ClientCache();
    Client defaultClient = clientCache.getClient(configuration,
        defaultSocketFactory, ObjectWritable.class);
    Client criticalScmClient = clientCache.getClient(configuration,
        scmLocationSocketFactory, ObjectWritable.class);

    try {
      assertNotSame(defaultClient, criticalScmClient);
    } finally {
      clientCache.stopClient(defaultClient);
      clientCache.stopClient(criticalScmClient);
    }
  }
}
