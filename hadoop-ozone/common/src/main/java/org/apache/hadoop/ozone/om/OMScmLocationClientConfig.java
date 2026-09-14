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

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_MAX_RETRY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_MAX_RETRY_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_RETRY_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_RETRY_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT_RETRIES;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT_RETRIES_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_MAX_RETRY_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_MAX_RETRY_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_RPC_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SCM_LOCATION_CLIENT_RPC_TIMEOUT_DEFAULT;

import java.util.concurrent.TimeUnit;
import javax.net.SocketFactory;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;

/**
 * Builds the configuration used by OM's SCM location clients.
 *
 * <p>The default effective retry limit is the larger of the configured retry
 * count and retry timeout divided by retry interval:
 * <pre>
 * max(3, 6s / 2s) = 3
 * </pre>
 * For failures that always trigger failover, this allows four attempts. The
 * configured timeout envelopes are:
 * <pre>
 * established connection: 4 * 30s + 3 * 2s = 126s
 * including TCP connect:   4 * (5s + 30s) + 3 * 2s = 146s
 * </pre>
 *
 * <p>Hadoop tracks retries and failovers separately. A sequence of three
 * retry-without-failover responses followed by three failover failures can
 * therefore make seven attempts. Its configured timeout envelopes are:
 * <pre>
 * established connection: 7 * 30s + 6 * 2s = 222s
 * including TCP connect:   7 * (5s + 30s) + 6 * 2s = 257s
 * </pre>
 * Actual calls can complete sooner. These calculations are not caller-side
 * deadlines and exclude time outside the configured connect and RPC waits.
 */
public final class OMScmLocationClientConfig {
  private OMScmLocationClientConfig() {
  }

  public static OzoneConfiguration createScmClientConfiguration(
      OzoneConfiguration configuration) {
    OzoneConfiguration scmClientConfiguration =
        new OzoneConfiguration(configuration);
    copy(configuration, scmClientConfiguration,
        OZONE_OM_SCM_LOCATION_CLIENT_RPC_TIMEOUT,
        OZONE_OM_SCM_LOCATION_CLIENT_RPC_TIMEOUT_DEFAULT,
        OzoneConfigKeys.HDDS_SCM_CLIENT_RPC_TIME_OUT);
    copyTimeDurationInMillis(configuration, scmClientConfiguration,
        OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT,
        OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT_DEFAULT,
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY);
    copy(configuration, scmClientConfiguration,
        OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT_RETRIES,
        OZONE_OM_SCM_LOCATION_CLIENT_IPC_CONNECT_TIMEOUT_RETRIES_DEFAULT,
        CommonConfigurationKeysPublic
            .IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY);
    copy(configuration, scmClientConfiguration,
        OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_MAX_RETRY,
        OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_MAX_RETRY_DEFAULT,
        OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_MAX_RETRY);
    copy(configuration, scmClientConfiguration,
        OZONE_OM_SCM_LOCATION_CLIENT_MAX_RETRY_TIMEOUT,
        OZONE_OM_SCM_LOCATION_CLIENT_MAX_RETRY_TIMEOUT_DEFAULT,
        OzoneConfigKeys.HDDS_SCM_CLIENT_MAX_RETRY_TIMEOUT);
    copy(configuration, scmClientConfiguration,
        OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_RETRY_INTERVAL,
        OZONE_OM_SCM_LOCATION_CLIENT_FAILOVER_RETRY_INTERVAL_DEFAULT,
        OzoneConfigKeys.HDDS_SCM_CLIENT_FAILOVER_RETRY_INTERVAL);
    return scmClientConfiguration;
  }

  /**
   * Creates a socket factory with a separate Hadoop IPC client cache entry so
   * the SCM location connection timeout is not shared with other clients.
   */
  static SocketFactory createSocketFactory(OzoneConfiguration configuration) {
    return new IdentitySocketFactory(
        NetUtils.getDefaultSocketFactory(configuration));
  }

  private static void copy(OzoneConfiguration source,
      OzoneConfiguration target, String sourceKey, String defaultValue,
      String targetKey) {
    target.set(targetKey, source.get(sourceKey, defaultValue));
  }

  private static void copyTimeDurationInMillis(OzoneConfiguration source,
      OzoneConfiguration target, String sourceKey, String defaultValue,
      String targetKey) {
    long value = source.getTimeDuration(sourceKey, defaultValue,
        TimeUnit.MILLISECONDS);
    target.setInt(targetKey, Math.toIntExact(value));
  }
}
