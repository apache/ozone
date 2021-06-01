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
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProtoOrBuilder;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import org.apache.commons.lang3.StringUtils;
import static org.apache.hadoop.hdds.DFSConfigKeysLegacy.DFS_DATANODE_DNS_INTERFACE_KEY;
import static org.apache.hadoop.hdds.DFSConfigKeysLegacy.DFS_DATANODE_DNS_NAMESERVER_KEY;
import static org.apache.hadoop.hdds.DFSConfigKeysLegacy.DFS_DATANODE_HOST_NAME_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_ADDRESS_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;

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
  public static Collection<InetSocketAddress> getScmAddressForClients(
      ConfigurationSource conf) {

    if (SCMHAUtils.getScmServiceId(conf) != null) {
      List<SCMNodeInfo> scmNodeInfoList = SCMNodeInfo.buildNodeInfo(conf);
      Collection<InetSocketAddress> scmAddressList =
          new HashSet<>(scmNodeInfoList.size());
      for (SCMNodeInfo scmNodeInfo : scmNodeInfoList) {
        if (scmNodeInfo.getScmClientAddress() == null) {
          throw new ConfigurationException("Ozone scm client address is not " +
              "set for SCM service-id " + scmNodeInfo.getServiceId() +
              "node-id" + scmNodeInfo.getNodeId());
        }
        scmAddressList.add(
            NetUtils.createSocketAddr(scmNodeInfo.getScmClientAddress()));
      }
      return scmAddressList;
    } else {
      String address = conf.getTrimmed(OZONE_SCM_CLIENT_ADDRESS_KEY);
      int port = -1;

      if (address == null) {
        // fall back to ozone.scm.names for non-ha
        Collection<String> scmAddresses =
            conf.getTrimmedStringCollection(OZONE_SCM_NAMES);

        if (scmAddresses.isEmpty()) {
          throw new ConfigurationException("Ozone scm client address is not " +
              "set. Configure one of these config " +
              OZONE_SCM_CLIENT_ADDRESS_KEY + ", " + OZONE_SCM_NAMES);
        }

        if (scmAddresses.size() > 1) {
          throw new ConfigurationException("For non-HA SCM " + OZONE_SCM_NAMES
              + " should be set with single address");
        }

        address = scmAddresses.iterator().next();

        port = conf.getInt(OZONE_SCM_CLIENT_PORT_KEY,
            OZONE_SCM_CLIENT_PORT_DEFAULT);
      } else {
        port = getHostPort(address)
            .orElse(conf.getInt(OZONE_SCM_CLIENT_PORT_KEY,
                OZONE_SCM_CLIENT_PORT_DEFAULT));
      }

      return Collections.singletonList(
          NetUtils.createSocketAddr(getHostName(address).get() + ":" + port));
    }
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
  public static Optional<String> getHostNameFromConfigKeys(
      ConfigurationSource conf,
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
      ConfigurationSource conf, String... keys) {
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
  public static Collection<InetSocketAddress> getSCMAddressForDatanodes(
      ConfigurationSource conf) {

    // First check HA style config, if not defined fall back to OZONE_SCM_NAMES

    if (SCMHAUtils.getScmServiceId(conf) != null) {
      List<SCMNodeInfo> scmNodeInfoList = SCMNodeInfo.buildNodeInfo(conf);
      Collection<InetSocketAddress> scmAddressList =
          new HashSet<>(scmNodeInfoList.size());
      for (SCMNodeInfo scmNodeInfo : scmNodeInfoList) {
        scmAddressList.add(
            NetUtils.createSocketAddr(scmNodeInfo.getScmDatanodeAddress()));
      }
      return scmAddressList;
    } else {
      // fall back to OZONE_SCM_NAMES.
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
            .orElse(conf.getInt(OZONE_SCM_DATANODE_PORT_KEY,
                OZONE_SCM_DATANODE_PORT_DEFAULT));
        InetSocketAddress addr = NetUtils.createSocketAddr(hostname.get(),
            port);
        addresses.add(addr);
      }

      if (addresses.size() > 1) {
        LOG.warn("When SCM HA is configured, configure {} appended with " +
            "serviceId and nodeId. {} is deprecated.", OZONE_SCM_ADDRESS_KEY,
            OZONE_SCM_NAMES);
      }
      return addresses;
    }
  }

  /**
   * Retrieve the socket addresses of recon.
   *
   * @return Recon address
   * @throws IllegalArgumentException If the configuration is invalid
   */
  public static InetSocketAddress getReconAddresses(
      ConfigurationSource conf) {
    String name = conf.get(OZONE_RECON_ADDRESS_KEY);
    if (StringUtils.isEmpty(name)) {
      return null;
    }
    Optional<String> hostname = getHostName(name);
    if (!hostname.isPresent()) {
      throw new IllegalArgumentException("Invalid hostname for Recon: "
          + name);
    }
    int port = getHostPort(name).orElse(OZONE_RECON_DATANODE_PORT_DEFAULT);
    return NetUtils.createSocketAddr(hostname.get(), port);
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
  public static String getHostName(ConfigurationSource conf)
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
      ContainerCommandRequestProtoOrBuilder proto) {
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
    case DeleteBlock:
    case DeleteChunk:
    case GetBlock:
    case GetCommittedBlockLength:
    case GetSmallFile:
    case PutBlock:
    case PutSmallFile:
    case ReadChunk:
    case WriteChunk:
      return true;
    default:
      return false;
    }
  }

  public static boolean requireContainerToken(
      ContainerProtos.Type cmdType) {
    switch (cmdType) {
    case CloseContainer:
    case CreateContainer:
    case DeleteContainer:
    case ReadContainer:
    case UpdateContainer:
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
  public static BlockID getBlockID(ContainerCommandRequestProtoOrBuilder msg) {
    ContainerProtos.DatanodeBlockID blockID = null;
    switch (msg.getCmdType()) {
    case DeleteBlock:
      if (msg.hasDeleteBlock()) {
        blockID = msg.getDeleteBlock().getBlockID();
      }
      break;
    case DeleteChunk:
      if (msg.hasDeleteChunk()) {
        blockID = msg.getDeleteChunk().getBlockID();
      }
      break;
    case GetBlock:
      if (msg.hasGetBlock()) {
        blockID = msg.getGetBlock().getBlockID();
      }
      break;
    case GetCommittedBlockLength:
      if (msg.hasGetCommittedBlockLength()) {
        blockID = msg.getGetCommittedBlockLength().getBlockID();
      }
      break;
    case GetSmallFile:
      if (msg.hasGetSmallFile()) {
        blockID = msg.getGetSmallFile().getBlock().getBlockID();
      }
      break;
    case ListChunk:
      if (msg.hasListChunk()) {
        blockID = msg.getListChunk().getBlockID();
      }
      break;
    case PutBlock:
      if (msg.hasPutBlock()) {
        blockID = msg.getPutBlock().getBlockData().getBlockID();
      }
      break;
    case PutSmallFile:
      if (msg.hasPutSmallFile()) {
        blockID = msg.getPutSmallFile().getBlock().getBlockData().getBlockID();
      }
      break;
    case ReadChunk:
      if (msg.hasReadChunk()) {
        blockID = msg.getReadChunk().getBlockID();
      }
      break;
    case WriteChunk:
      if (msg.hasWriteChunk()) {
        blockID = msg.getWriteChunk().getBlockID();
      }
      break;
    default:
      break;
    }

    return blockID != null
        ? BlockID.getFromProtobuf(blockID)
        : null;
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
   * Get the current time in milliseconds.
   * @return the current time in milliseconds.
   */
  public static long getTime() {
    return System.currentTimeMillis();
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

  public static File createDir(String dirPath) {
    File dirFile = new File(dirPath);
    if (!dirFile.mkdirs() && !dirFile.exists()) {
      throw new IllegalArgumentException("Unable to create path: " + dirFile);
    }
    return dirFile;
  }

  /**
   * Leverages the Configuration.getPassword method to attempt to get
   * passwords from the CredentialProvider API before falling back to
   * clear text in config - if falling back is allowed.
   * @param conf Configuration instance
   * @param alias name of the credential to retrieve
   * @return String credential value or null
   */
  static String getPassword(ConfigurationSource conf, String alias) {
    String password = null;
    try {
      char[] passchars = conf.getPassword(alias);
      if (passchars != null) {
        password = new String(passchars);
      }
    } catch (IOException ioe) {
      LOG.warn("Setting password to null since IOException is caught"
          + " when getting password", ioe);

      password = null;
    }
    return password;
  }

  /**
   * Utility string formatter method to display SCM roles.
   *
   * @param nodes
   * @return
   */
  public static String format(List<String> nodes) {
    StringBuilder sb = new StringBuilder();
    for (String node : nodes) {
      String[] x = node.split(":");
      sb.append(String
          .format("{ HostName : %s, Ratis Port : %s, Role : %s } ", x[0], x[1],
              x[2]));
    }
    return sb.toString();
  }
}
