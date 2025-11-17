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

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.hdds.HddsUtils.getHostNameFromConfigKeys;
import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_CONTAINER_REPORT_QUEUE_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_PREFIX;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_EVENT_THREAD_POOL_SIZE_DEFAULT;

import jakarta.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.BlockingQueue;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.security.RootCARotationManager;
import org.apache.hadoop.hdds.scm.server.ContainerReportQueue;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReport;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SCM utility class.
 */
public final class ScmUtils {
  private static final Logger LOG = LoggerFactory
      .getLogger(ScmUtils.class);

  private ScmUtils() {
  }

  public static InetSocketAddress getScmBlockProtocolServerAddress(
      OzoneConfiguration conf, String localScmServiceId, String nodeId) {
    String bindHostKey = ConfUtils.addKeySuffixes(
        OZONE_SCM_BLOCK_CLIENT_BIND_HOST_KEY, localScmServiceId, nodeId);
    final Optional<String> host = getHostNameFromConfigKeys(conf, bindHostKey);

    String addressKey = ConfUtils.addKeySuffixes(
        OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, localScmServiceId, nodeId);
    final OptionalInt port = getPortNumberFromConfigKeys(conf, addressKey);

    if (port.isPresent()) {
      logWarn(OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
          OZONE_SCM_BLOCK_CLIENT_PORT_KEY);
    }
    return NetUtils.createSocketAddr(
        host.orElse(
            OZONE_SCM_BLOCK_CLIENT_BIND_HOST_DEFAULT) + ":" +
            port.orElse(conf.getInt(
                ConfUtils.addKeySuffixes(OZONE_SCM_BLOCK_CLIENT_PORT_KEY,
                    localScmServiceId, nodeId),
                conf.getInt(OZONE_SCM_BLOCK_CLIENT_PORT_KEY,
                    OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT))));
  }

  public static String getScmBlockProtocolServerAddressKey(
      String serviceId, String nodeId) {
    return ConfUtils.addKeySuffixes(OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
        serviceId, nodeId);
  }

  public static InetSocketAddress getClientProtocolServerAddress(
      OzoneConfiguration conf, String localScmServiceId, String nodeId) {
    String bindHostKey = ConfUtils.addKeySuffixes(
        OZONE_SCM_CLIENT_BIND_HOST_KEY, localScmServiceId, nodeId);

    final String host = getHostNameFromConfigKeys(conf, bindHostKey)
        .orElse(OZONE_SCM_CLIENT_BIND_HOST_DEFAULT);

    String addressKey = ConfUtils.addKeySuffixes(
        OZONE_SCM_CLIENT_ADDRESS_KEY, localScmServiceId, nodeId);

    OptionalInt port = getPortNumberFromConfigKeys(conf, addressKey);

    if (port.isPresent()) {
      logWarn(OZONE_SCM_CLIENT_ADDRESS_KEY, OZONE_SCM_CLIENT_PORT_KEY);
    }

    return NetUtils.createSocketAddr(host + ":" +
        port.orElse(
            conf.getInt(ConfUtils.addKeySuffixes(OZONE_SCM_CLIENT_PORT_KEY,
                localScmServiceId, nodeId),
            conf.getInt(OZONE_SCM_CLIENT_PORT_KEY,
                OZONE_SCM_CLIENT_PORT_DEFAULT))));
  }

  public static String getClientProtocolServerAddressKey(
      String serviceId, String nodeId) {
    return ConfUtils.addKeySuffixes(OZONE_SCM_CLIENT_ADDRESS_KEY, serviceId,
        nodeId);
  }

  public static InetSocketAddress getScmDataNodeBindAddress(
      ConfigurationSource conf, String localScmServiceId, String nodeId) {
    String bindHostKey = ConfUtils.addKeySuffixes(
        OZONE_SCM_DATANODE_BIND_HOST_KEY, localScmServiceId, nodeId);
    final Optional<String> host = getHostNameFromConfigKeys(conf, bindHostKey);
    String addressKey = ConfUtils.addKeySuffixes(
        OZONE_SCM_DATANODE_ADDRESS_KEY, localScmServiceId, nodeId);
    final OptionalInt port = getPortNumberFromConfigKeys(conf, addressKey);

    if (port.isPresent()) {
      logWarn(OZONE_SCM_DATANODE_ADDRESS_KEY, OZONE_SCM_DATANODE_PORT_KEY);
    }

    return NetUtils.createSocketAddr(
        host.orElse(OZONE_SCM_DATANODE_BIND_HOST_DEFAULT) + ":" +
            port.orElse(conf.getInt(ConfUtils.addKeySuffixes(
                OZONE_SCM_DATANODE_PORT_KEY, localScmServiceId, nodeId),
                conf.getInt(OZONE_SCM_DATANODE_PORT_KEY,
                    OZONE_SCM_DATANODE_PORT_DEFAULT))));
  }

  public static String getScmDataNodeBindAddressKey(
      String serviceId, String nodeId) {
    return ConfUtils.addKeySuffixes(
        OZONE_SCM_DATANODE_ADDRESS_KEY,
        serviceId, nodeId);
  }

  private static void logWarn(String confKey, String portKey) {
    LOG.warn("ConfigKey {} is deprecated, For configuring different ports " +
            "for each SCM use PortConfigKey {} appended with serviceId and " +
            "nodeId. If want to configure same port configure {}", confKey,
        portKey, portKey);
  }

  public static boolean shouldRemovePeers(final ConfigurationSource conf) {
    int pipelineLimitPerDn =
        conf.getInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT,
            ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT_DEFAULT);
    return (1 != pipelineLimitPerDn && conf
        .getBoolean(ScmConfigKeys.OZONE_SCM_DATANODE_DISALLOW_SAME_PEERS,
            ScmConfigKeys.OZONE_SCM_DATANODE_DISALLOW_SAME_PEERS_DEFAULT));
  }

  @Nonnull
  public static List<BlockingQueue<ContainerReport>> initContainerReportQueue(
      OzoneConfiguration configuration) {
    int threadPoolSize = configuration.getInt(getContainerReportConfPrefix()
            + ".thread.pool.size",
        OZONE_SCM_EVENT_THREAD_POOL_SIZE_DEFAULT);
    int queueSize = configuration.getInt(getContainerReportConfPrefix()
            + ".queue.size",
        OZONE_SCM_EVENT_CONTAINER_REPORT_QUEUE_SIZE_DEFAULT);
    List<BlockingQueue<ContainerReport>> queues = new ArrayList<>();
    for (int i = 0; i < threadPoolSize; ++i) {
      queues.add(new ContainerReportQueue(queueSize));
    }
    return queues;
  }
  
  public static String getContainerReportConfPrefix() {
    return OZONE_SCM_EVENT_PREFIX +
        StringUtils.camelize(SCMEvents.CONTAINER_REPORT.getName()
            + "_OR_"
            + SCMEvents.INCREMENTAL_CONTAINER_REPORT.getName());
  }

  public static void checkIfCertSignRequestAllowed(
      RootCARotationManager rotationManager, boolean isScmCertRenew,
      OzoneConfiguration config, String operation) throws SCMException {
    if (rotationManager != null) {
      if (rotationManager.isRotationInProgress() && !isScmCertRenew) {
        throw new SCMException("Root CA and Sub CA rotation is in-progress." +
            " Please try the operation later again.",
            SCMException.ResultCodes.CA_ROTATION_IN_PROGRESS);
      }
      if (rotationManager.isPostRotationInProgress()) {
        SecurityConfig securityConfig = new SecurityConfig(config);
        throw new SCMException("The operation " + operation +
            " is prohibited due to root CA " +
            "and sub CA rotation have just finished. " +
            "The prohibition state will last at most " +
            securityConfig.getRootCaCertificatePollingInterval() + ". " +
            "Please try the operation later again.",
            SCMException.ResultCodes.CA_ROTATION_IN_POST_PROGRESS);
      }
    }
  }
}
