/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds;

import javax.management.ObjectName;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolPB;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.WriteChunkRequestProto;
import org.apache.hadoop.hdds.scm.protocolPB.ScmBlockLocationProtocolPB;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;

import com.google.common.net.HostAndPort;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDDS specific stateless utility functions.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public final class HddsUtils {


  private static final Logger LOG = LoggerFactory.getLogger(HddsUtils.class);

  /**
   * The service ID of the solitary Ozone SCM service.
   */
  public static final String OZONE_SCM_SERVICE_ID = "OzoneScmService";
  public static final String OZONE_SCM_SERVICE_INSTANCE_ID =
      "OzoneScmServiceInstance";
  private static final TimeZone UTC_ZONE = TimeZone.getTimeZone("UTC");

  private static final String MULTIPLE_SCM_NOT_YET_SUPPORTED =
      ScmConfigKeys.OZONE_SCM_NAMES + " must contain a single hostname."
          + " Multiple SCM hosts are currently unsupported";

  private static final int NO_PORT = -1;

  private HddsUtils() {
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM.
   *
   * @return Target {@code InetSocketAddress} for the SCM client endpoint.
   */
  public static InetSocketAddress getScmAddressForClients(Configuration conf) {
    Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);

    if (!host.isPresent()) {
      // Fallback to Ozone SCM name
      host = Optional.of(getSingleSCMAddress(conf).getHostName());
    }

    final int port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT);

    return NetUtils.createSocketAddr(host.get() + ":" + port);
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM for block service. If
   * {@link ScmConfigKeys#OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY} is not defined
   * then {@link ScmConfigKeys#OZONE_SCM_CLIENT_ADDRESS_KEY} is used. If neither
   * is defined then {@link ScmConfigKeys#OZONE_SCM_NAMES} is used.
   *
   * @return Target {@code InetSocketAddress} for the SCM block client endpoint.
   * @throws IllegalArgumentException if configuration is not defined.
   */
  public static InetSocketAddress getScmAddressForBlockClients(
      Configuration conf) {
    Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);

    if (!host.isPresent()) {
      // Fallback to Ozone SCM name
      host = Optional.of(getSingleSCMAddress(conf).getHostName());
    }

    final int port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_PORT_DEFAULT);

    return NetUtils.createSocketAddr(host.get() + ":" + port);
  }

  /**
   * Create a scm security client.
   * @param conf    - Ozone configuration.
   *
   * @return {@link SCMSecurityProtocol}
   * @throws IOException
   */
  public static SCMSecurityProtocolClientSideTranslatorPB getScmSecurityClient(
      OzoneConfiguration conf) throws IOException {
    RPC.setProtocolEngine(conf, SCMSecurityProtocolPB.class,
        ProtobufRpcEngine.class);
    long scmVersion =
        RPC.getProtocolVersion(ScmBlockLocationProtocolPB.class);
    InetSocketAddress address =
        getScmAddressForSecurityProtocol(conf);
    RetryPolicy retryPolicy =
        RetryPolicies.retryForeverWithFixedSleep(
            1000, TimeUnit.MILLISECONDS);
    return new SCMSecurityProtocolClientSideTranslatorPB(
        RPC.getProtocolProxy(SCMSecurityProtocolPB.class, scmVersion,
            address, UserGroupInformation.getCurrentUser(),
            conf, NetUtils.getDefaultSocketFactory(conf),
            Client.getRpcTimeout(conf), retryPolicy).getProxy());
  }

  /**
   * Retrieve the hostname, trying the supplied config keys in order.
   * Each config value may be absent, or if present in the format
   * host:port (the :port part is optional).
   *
   * @param conf  - Conf
   * @param keys a list of configuration key names.
   *
   * @return first hostname component found from the given keys, or absent.
   * @throws IllegalArgumentException if any values are not in the 'host'
   *             or host:port format.
   */
  public static Optional<String> getHostNameFromConfigKeys(Configuration conf,
      String... keys) {
    for (final String key : keys) {
      final String value = conf.getTrimmed(key);
      final Optional<String> hostName = getHostName(value);
      if (hostName.isPresent()) {
        return hostName;
      }
    }
    return Optional.empty();
  }

  /**
   * Gets the hostname or Indicates that it is absent.
   * @param value host or host:port
   * @return hostname
   */
  public static Optional<String> getHostName(String value) {
    if ((value == null) || value.isEmpty()) {
      return Optional.empty();
    }
    String hostname = value.replaceAll("\\:[0-9]+$", "");
    if (hostname.length() == 0) {
      return Optional.empty();
    } else {
      return Optional.of(hostname);
    }
  }

  /**
   * Gets the port if there is one, returns empty {@code OptionalInt} otherwise.
   * @param value  String in host:port format.
   * @return Port
   */
  public static OptionalInt getHostPort(String value) {
    if ((value == null) || value.isEmpty()) {
      return OptionalInt.empty();
    }
    int port = HostAndPort.fromString(value).getPortOrDefault(NO_PORT);
    if (port == NO_PORT) {
      return OptionalInt.empty();
    } else {
      return OptionalInt.of(port);
    }
  }

  /**
   * Retrieve the port number, trying the supplied config keys in order.
   * Each config value may be absent, or if present in the format
   * host:port (the :port part is optional).
   *
   * @param conf Conf
   * @param keys a list of configuration key names.
   *
   * @return first port number component found from the given keys, or absent.
   * @throws IllegalArgumentException if any values are not in the 'host'
   *             or host:port format.
   */
  public static OptionalInt getPortNumberFromConfigKeys(
      Configuration conf, String... keys) {
    for (final String key : keys) {
      final String value = conf.getTrimmed(key);
      final OptionalInt hostPort = getHostPort(value);
      if (hostPort.isPresent()) {
        return hostPort;
      }
    }
    return OptionalInt.empty();
  }

  /**
   * Retrieve the socket addresses of all storage container managers.
   *
   * @return A collection of SCM addresses
   * @throws IllegalArgumentException If the configuration is invalid
   */
  public static Collection<InetSocketAddress> getSCMAddresses(
      Configuration conf) {
    Collection<String> names =
        conf.getTrimmedStringCollection(ScmConfigKeys.OZONE_SCM_NAMES);
    if (names.isEmpty()) {
      throw new IllegalArgumentException(ScmConfigKeys.OZONE_SCM_NAMES
          + " need to be a set of valid DNS names or IP addresses."
          + " Empty address list found.");
    }

    Collection<InetSocketAddress> addresses = new HashSet<>(names.size());
    for (String address : names) {
      Optional<String> hostname = getHostName(address);
      if (!hostname.isPresent()) {
        throw new IllegalArgumentException("Invalid hostname for SCM: "
            + address);
      }
      int port = getHostPort(address)
          .orElse(ScmConfigKeys.OZONE_SCM_DEFAULT_PORT);
      InetSocketAddress addr = NetUtils.createSocketAddr(hostname.get(), port);
      addresses.add(addr);
    }
    return addresses;
  }

  /**
   * Retrieve the address of the only SCM (as currently multiple ones are not
   * supported).
   *
   * @return SCM address
   * @throws IllegalArgumentException if {@code conf} has more than one SCM
   *         address or it has none
   */
  public static InetSocketAddress getSingleSCMAddress(Configuration conf) {
    Collection<InetSocketAddress> singleton = getSCMAddresses(conf);
    Preconditions.checkArgument(singleton.size() == 1,
        MULTIPLE_SCM_NOT_YET_SUPPORTED);
    return singleton.iterator().next();
  }

  /**
   * Returns the hostname for this datanode. If the hostname is not
   * explicitly configured in the given config, then it is determined
   * via the DNS class.
   *
   * @param conf Configuration
   *
   * @return the hostname (NB: may not be a FQDN)
   * @throws UnknownHostException if the dfs.datanode.dns.interface
   *    option is used and the hostname can not be determined
   */
  public static String getHostName(Configuration conf)
      throws UnknownHostException {
    String name = conf.get(DFS_DATANODE_HOST_NAME_KEY);
    if (name == null) {
      String dnsInterface = conf.get(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_DNS_INTERFACE_KEY);
      String nameServer = conf.get(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_DNS_NAMESERVER_KEY);
      boolean fallbackToHosts = false;

      if (dnsInterface == null) {
        // Try the legacy configuration keys.
        dnsInterface = conf.get(DFS_DATANODE_DNS_INTERFACE_KEY);
        nameServer = conf.get(DFS_DATANODE_DNS_NAMESERVER_KEY);
      } else {
        // If HADOOP_SECURITY_DNS_* is set then also attempt hosts file
        // resolution if DNS fails. We will not use hosts file resolution
        // by default to avoid breaking existing clusters.
        fallbackToHosts = true;
      }

      name = DNS.getDefaultHost(dnsInterface, nameServer, fallbackToHosts);
    }
    return name;
  }

  /**
   * Checks if the container command is read only or not.
   * @param proto ContainerCommand Request proto
   * @return True if its readOnly , false otherwise.
   */
  public static boolean isReadOnly(
      ContainerProtos.ContainerCommandRequestProto proto) {
    switch (proto.getCmdType()) {
    case ReadContainer:
    case ReadChunk:
    case ListBlock:
    case GetBlock:
    case GetSmallFile:
    case ListContainer:
    case ListChunk:
    case GetCommittedBlockLength:
      return true;
    case CloseContainer:
    case WriteChunk:
    case UpdateContainer:
    case CompactChunk:
    case CreateContainer:
    case DeleteChunk:
    case DeleteContainer:
    case DeleteBlock:
    case PutBlock:
    case PutSmallFile:
    default:
      return false;
    }
  }

  /**
   * Not all datanode container cmd protocol has embedded ozone block token.
   * Block token are issued by Ozone Manager and return to Ozone client to
   * read/write data on datanode via input/output stream.
   * Ozone datanode uses this helper to decide which command requires block
   * token.
   * @return true if it is a cmd that block token should be checked when
   * security is enabled
   * false if block token does not apply to the command.
   *
   */
  public static boolean requireBlockToken(
      ContainerProtos.Type cmdType) {
    switch (cmdType) {
    case ReadChunk:
    case GetBlock:
    case WriteChunk:
    case PutBlock:
    case PutSmallFile:
    case GetSmallFile:
      return true;
    default:
      return false;
    }
  }

  /**
   * Return the block ID of container commands that are related to blocks.
   * @param msg container command
   * @return block ID.
   */
  public static BlockID getBlockID(ContainerCommandRequestProto msg) {
    switch (msg.getCmdType()) {
    case ReadChunk:
      if (msg.hasReadChunk()) {
        return BlockID.getFromProtobuf(msg.getReadChunk().getBlockID());
      }
      return null;
    case GetBlock:
      if (msg.hasGetBlock()) {
        return BlockID.getFromProtobuf(msg.getGetBlock().getBlockID());
      }
      return null;
    case WriteChunk:
      if (msg.hasWriteChunk()) {
        return BlockID.getFromProtobuf(msg.getWriteChunk().getBlockID());
      }
      return null;
    case PutBlock:
      if (msg.hasPutBlock()) {
        return BlockID.getFromProtobuf(msg.getPutBlock().getBlockData()
            .getBlockID());
      }
      return null;
    case PutSmallFile:
      if (msg.hasPutSmallFile()) {
        return BlockID.getFromProtobuf(msg.getPutSmallFile().getBlock()
            .getBlockData().getBlockID());
      }
      return null;
    case GetSmallFile:
      if (msg.hasGetSmallFile()) {
        return BlockID.getFromProtobuf(msg.getGetSmallFile().getBlock()
            .getBlockID());
      }
      return null;
    default:
      return null;
    }
  }

  /**
   * Register the provided MBean with additional JMX ObjectName properties.
   * If additional properties are not supported then fallback to registering
   * without properties.
   *
   * @param serviceName - see {@link MBeans#register}
   * @param mBeanName - see {@link MBeans#register}
   * @param jmxProperties - additional JMX ObjectName properties.
   * @param mBean - the MBean to register.
   * @return the named used to register the MBean.
   */
  public static ObjectName registerWithJmxProperties(
      String serviceName, String mBeanName, Map<String, String> jmxProperties,
      Object mBean) {
    try {

      // Check support for registering with additional properties.
      final Method registerMethod = MBeans.class.getMethod(
          "register", String.class, String.class,
          Map.class, Object.class);

      return (ObjectName) registerMethod.invoke(
          null, serviceName, mBeanName, jmxProperties, mBean);

    } catch (NoSuchMethodException | IllegalAccessException |
        InvocationTargetException e) {

      // Fallback
      if (LOG.isTraceEnabled()) {
        LOG.trace("Registering MBean {} without additional properties {}",
            mBeanName, jmxProperties);
      }
      return MBeans.register(serviceName, mBeanName, mBean);
    }
  }

  /**
   * Get the current UTC time in milliseconds.
   * @return the current UTC time in milliseconds.
   */
  public static long getUtcTime() {
    return Calendar.getInstance(UTC_ZONE).getTimeInMillis();
  }

  /**
   * Retrieve the socket address that should be used by clients to connect
   * to the SCM for
   * {@link org.apache.hadoop.hdds.protocol.SCMSecurityProtocol}. If
   * {@link ScmConfigKeys#OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY} is not defined
   * then {@link ScmConfigKeys#OZONE_SCM_CLIENT_ADDRESS_KEY} is used. If neither
   * is defined then {@link ScmConfigKeys#OZONE_SCM_NAMES} is used.
   *
   * @param conf
   * @return Target {@code InetSocketAddress} for the SCM block client endpoint.
   * @throws IllegalArgumentException if configuration is not defined or invalid
   */
  public static InetSocketAddress getScmAddressForSecurityProtocol(
      Configuration conf) {
    Optional<String> host = getHostNameFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_ADDRESS_KEY,
        ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY);

    if (!host.isPresent()) {
      // Fallback to Ozone SCM name
      host = Optional.of(getSingleSCMAddress(conf).getHostName());
    }

    final int port = getPortNumberFromConfigKeys(conf,
        ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_KEY)
        .orElse(ScmConfigKeys.OZONE_SCM_SECURITY_SERVICE_PORT_DEFAULT);

    return NetUtils.createSocketAddr(host.get() + ":" + port);
  }

  /**
   * Initialize hadoop metrics system for Ozone servers.
   * @param configuration OzoneConfiguration to use.
   * @param serverName    The logical name of the server components.
   */
  public static MetricsSystem initializeMetrics(
      OzoneConfiguration configuration, String serverName) {
    MetricsSystem metricsSystem = DefaultMetricsSystem.initialize(serverName);
    try {
      JvmMetrics.create(serverName,
          configuration.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY),
          DefaultMetricsSystem.instance());
    } catch (MetricsException e) {
      LOG.info("Metrics source JvmMetrics already added to DataNode.");
    }
    return metricsSystem;
  }

  /**
   * Basic validation for {@code path}: checks that it is a descendant of
   * (or the same as) the given {@code ancestor}.
   * @param path the path to be validated
   * @param ancestor a trusted path that is supposed to be the ancestor of
   *     {@code path}
   * @throws NullPointerException if either {@code path} or {@code ancestor} is
   *     null
   * @throws IllegalArgumentException if {@code ancestor} is not really the
   *     ancestor of {@code path}
   */
  public static void validatePath(Path path, Path ancestor) {
    Preconditions.checkNotNull(path,
        "Path should not be null");
    Preconditions.checkNotNull(ancestor,
        "Ancestor should not be null");
    Preconditions.checkArgument(
        path.normalize().startsWith(ancestor.normalize()),
        "Path should be a descendant of %s", ancestor);
  }

  public static String writeChunkToString(WriteChunkRequestProto wc,
                                          long contId, String location) {
    Preconditions.checkNotNull(wc);
    StringBuilder builder = new StringBuilder();

    builder.append("cmd=");
    builder.append(ContainerProtos.Type.WriteChunk.toString());

    builder.append(", container id=");
    builder.append(contId);

    builder.append(", blockid=");
    builder.append(wc.getBlockID().getContainerID());
    builder.append(":localid=");
    builder.append(wc.getBlockID().getLocalID());

    builder.append(", chunk=");
    builder.append(wc.getChunkData().getChunkName());
    builder.append(":offset=");
    builder.append(wc.getChunkData().getOffset());
    builder.append(":length=");
    builder.append(wc.getChunkData().getLen());

    builder.append(", container path=");
    builder.append(location);

    return builder.toString();
  }
}
