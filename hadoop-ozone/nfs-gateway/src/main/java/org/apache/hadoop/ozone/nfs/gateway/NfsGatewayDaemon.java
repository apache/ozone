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

package org.apache.hadoop.ozone.nfs.gateway;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.nfs.NfsExportBinding;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ImpersonationProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Starts the standalone, loopback-only Java NFS backend. */
public final class NfsGatewayDaemon {
  private static final Logger LOG = LoggerFactory.getLogger(NfsGatewayDaemon.class);
  private static final String EXPORT_PREFIX = "ozone.nfs.export.";

  private NfsGatewayDaemon() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 0) {
      throw new IllegalArgumentException("NFS gateway does not accept positional arguments");
    }
    OzoneConfiguration configuration = new OzoneConfiguration();
    try (NfsGatewayServer server = start(configuration)) {
      LOG.info("NFS gateway listening on {}", server.getPort());
      server.awaitTermination();
    }
  }

  static NfsGatewayServer start(OzoneConfiguration configuration) throws IOException {
    if (!OzoneSecurityUtil.isSecurityEnabled(configuration)) {
      throw new IOException("NFS gateway requires Ozone security to be enabled");
    }
    SecurityUtil.login(configuration, NfsGatewayConfigKeys.KERBEROS_KEYTAB,
        NfsGatewayConfigKeys.KERBEROS_PRINCIPAL);
    UserGroupInformation gatewayUser = UserGroupInformation.getLoginUser();
    ProxyUsers.refreshSuperUserGroupsConfiguration(configuration);
    ImpersonationProvider impersonationProvider = ProxyUsers.getDefaultImpersonationProvider();
    NfsCallerAuthorizer authorizer = new NfsCallerAuthorizer(gatewayUser, impersonationProvider);

    ScmInfo scmInfo = HAUtils.getScmInfo(configuration);
    String clusterId = scmInfo.getClusterId();
    if (clusterId == null || clusterId.isEmpty()) {
      throw new IOException("SCM did not provide a cluster ID for NFS export validation");
    }
    String omServiceId = OmUtils.getOzoneManagerServiceId(configuration);
    Map<ByteString, NfsObjectReader> readers = createReaders(configuration, clusterId, omServiceId, authorizer);
    Set<UUID> exportIds = new HashSet<>();
    readers.values().forEach(reader -> exportIds.add(uuid(reader.getExportUuid())));
    Map<String, Set<UUID>> allowedPeers = new HashMap<>();
    for (String fingerprint : configuration.getTrimmedStringCollection(
        NfsGatewayConfigKeys.GANESHA_CLIENT_CERTIFICATES)) {
      if (fingerprint.isEmpty()) {
        continue;
      }
      allowedPeers.put(fingerprint, Set.copyOf(exportIds));
    }
    if (allowedPeers.isEmpty()) {
      throw new IOException("At least one Ganesha client certificate fingerprint is required");
    }

    String bindAddress = configuration.getTrimmed(NfsGatewayConfigKeys.ADDRESS, "127.0.0.1");
    InetAddress address = InetAddress.getByName(bindAddress);
    if (!address.isLoopbackAddress()) {
      throw new IOException("NFS gateway must bind to a loopback address");
    }
    int port = configuration.getInt(NfsGatewayConfigKeys.PORT, 9878);
    if (port < 1 || port > 65535) {
      throw new IOException("Invalid NFS gateway port: " + port);
    }
    File certificate = requiredFile(configuration, NfsGatewayConfigKeys.SERVER_CERTIFICATE);
    File key = requiredFile(configuration, NfsGatewayConfigKeys.SERVER_PRIVATE_KEY);
    File trustedCa = requiredFile(configuration, NfsGatewayConfigKeys.TRUSTED_CA);
    String gatewayId = configuration.getTrimmed(NfsGatewayConfigKeys.GATEWAY_ID);
    if (gatewayId == null || gatewayId.isEmpty()) {
      throw new IOException("Missing NFS gateway configuration: " + NfsGatewayConfigKeys.GATEWAY_ID);
    }
    long leaseDurationMillis = TimeUnit.SECONDS.toMillis(configuration.getTimeDuration(
        NfsGatewayConfigKeys.EXPORT_LEASE_DURATION, 30, TimeUnit.SECONDS));
    NfsExportLeaseManager leaseManager = new NfsExportLeaseManager(readers, gatewayId, leaseDurationMillis);
    leaseManager.start();
    try {
      NfsGatewayServer server = new NfsGatewayServer(new InetSocketAddress(address, port), certificate, key,
          trustedCa, allowedPeers, new NfsGatewayService(readers, leaseManager), leaseManager);
      server.startKerberosRelogin(gatewayUser);
      return server;
    } catch (IOException | RuntimeException ex) {
      leaseManager.close();
      throw ex;
    }
  }

  private static Map<ByteString, NfsObjectReader> createReaders(OzoneConfiguration configuration,
      String clusterId, String omServiceId, NfsCallerAuthorizer authorizer) throws IOException {
    Set<String> exportNames = new TreeSet<>();
    for (String key : configuration.getConfigKeys()) {
      if (key.startsWith(EXPORT_PREFIX) && key.endsWith(".uuid")) {
        exportNames.add(key.substring(EXPORT_PREFIX.length(), key.length() - ".uuid".length()));
      }
    }
    if (exportNames.isEmpty()) {
      throw new IOException("No ozone.nfs.export.<name> bindings are configured");
    }
    Map<ByteString, NfsObjectReader> readers = new HashMap<>();
    for (String exportName : exportNames) {
      NfsExportBinding binding = NfsExportBinding.fromConfiguration(configuration, exportName);
      if (!clusterId.equals(binding.getClusterId())) {
        throw new IOException("NFS export " + exportName + " is configured for a different Ozone cluster");
      }
      NfsObjectReader reader = new NfsObjectReader(configuration, omServiceId, clusterId, binding, authorizer);
      if (readers.putIfAbsent(reader.getExportUuid(), reader) != null) {
        throw new IOException("Duplicate NFS export UUID: " + binding.getExportId());
      }
    }
    return Map.copyOf(readers);
  }

  private static File requiredFile(OzoneConfiguration configuration, String key) throws IOException {
    String value = configuration.getTrimmed(key);
    if (value == null || value.isEmpty()) {
      throw new IOException("Missing NFS gateway configuration: " + key);
    }
    File file = new File(value);
    if (!file.isFile() || !file.canRead()) {
      throw new IOException("NFS gateway file is not readable: " + key);
    }
    return file;
  }

  private static UUID uuid(ByteString encoded) {
    ByteBuffer buffer = encoded.asReadOnlyByteBuffer();
    return new UUID(buffer.getLong(), buffer.getLong());
  }
}
