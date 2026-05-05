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

package org.apache.hadoop.hdds;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_CLIENT_BIND_HOST_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_CLIENT_BIND_HOST_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_DNS_INTERFACE_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_DNS_NAMESERVER_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HOST_NAME_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_PORT_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEFAULT_SERVICE_ID;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_SERVICE_IDS_KEY;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ServiceException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.UUID;
import javax.management.ObjectName;
import org.apache.hadoop.conf.ConfigRedactor;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.ConfigurationException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProtoOrBuilder;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.DatanodeBlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.ipc_.RPC;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.ipc_.RpcException;
import org.apache.hadoop.ipc_.RpcNoSuchMethodException;
import org.apache.hadoop.ipc_.RpcNoSuchProtocolException;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.conf.OzoneServiceConfig;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.TextFormat;
import org.apache.ratis.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HDDS specific stateless utility functions.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public final class HddsUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HddsUtils.class);

  public static final String REDACTED_STRING = "<redacted>";
  public static final ByteString REDACTED = ByteString.copyFromUtf8(REDACTED_STRING);

  private static final int ONE_MB = SizeInBytes.valueOf("1m").getSizeInt();

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

    if (getScmServiceId(conf) != null) {
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
    if (hostname.isEmpty()) {
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
   * Retrieve a number, trying the supplied config keys in order.
   * Each config value may be absent
   *
   * @param conf Conf
   * @param keys a list of configuration key names.
   *
   * @return first number found from the given keys, or absent.
   */
  public static OptionalInt getNumberFromConfigKeys(
      ConfigurationSource conf, String... keys) {
    for (final String key : keys) {
      final String value = conf.getTrimmed(key);
      if (value != null) {
        return OptionalInt.of(Integer.parseInt(value));
      }
    }
    return OptionalInt.empty();
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
   * Returns the hostname for this datanode. If the hostname is not
   * explicitly configured in the given config, then it is determined
   * via the DNS class.
   *
   * @param conf Configuration
   *
   * @return the hostname (NB: may not be a FQDN)
   * @throws UnknownHostException if the hdds.datanode.dns.interface
   *    option is used and the hostname can not be determined
   */
  public static String getHostName(ConfigurationSource conf)
      throws UnknownHostException {
    String name = conf.get(HDDS_DATANODE_HOST_NAME_KEY);
    if (name == null) {
      String dnsInterface = conf.get(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_DNS_INTERFACE_KEY);
      String nameServer = conf.get(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_DNS_NAMESERVER_KEY);
      boolean fallbackToHosts = false;

      if (dnsInterface == null) {
        // Try the legacy configuration keys.
        dnsInterface = conf.get(HDDS_DATANODE_DNS_INTERFACE_KEY);
        nameServer = conf.get(HDDS_DATANODE_DNS_NAMESERVER_KEY);
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
   * Retrieve the socket address that is used by Datanode.
   * @param conf
   * @return Target InetSocketAddress for the Datanode service endpoint.
   */
  public static InetSocketAddress
      getDatanodeRpcAddress(ConfigurationSource conf) {
    final String host = getHostNameFromConfigKeys(conf,
        HDDS_DATANODE_CLIENT_BIND_HOST_KEY)
        .orElse(HDDS_DATANODE_CLIENT_BIND_HOST_DEFAULT);

    final int port = getPortNumberFromConfigKeys(conf,
        HDDS_DATANODE_CLIENT_ADDRESS_KEY)
        .orElse(conf.getInt(HDDS_DATANODE_CLIENT_PORT_KEY,
            HDDS_DATANODE_CLIENT_PORT_DEFAULT));

    return NetUtils.createSocketAddr(host + ":" + port);
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
    case ReadBlock:
    case ListBlock:
    case GetBlock:
    case GetSmallFile:
    case ListContainer:
    case ListChunk:
    case GetCommittedBlockLength:
    case GetContainerChecksumInfo:
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
    case StreamInit:
    case StreamWrite:
    case FinalizeBlock:
      return false;
    case Echo:
      return proto.getEcho().hasReadOnly() && proto.getEcho().getReadOnly();
    default:
      return false;
    }
  }

  /**
   * Returns true if the container is in open to write state
   * (OPEN or RECOVERING).
   *
   * @param state - container state
   */
  public static boolean isOpenToWriteState(State state) {
    return state == State.OPEN || state == State.RECOVERING;
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
  public static boolean requireBlockToken(Type cmdType) {
    switch (cmdType) {
    case DeleteBlock:
    case DeleteChunk:
    case GetBlock:
    case GetCommittedBlockLength:
    case GetSmallFile:
    case PutBlock:
    case PutSmallFile:
    case ReadChunk:
    case ReadBlock:
    case WriteChunk:
    case FinalizeBlock:
      return true;
    default:
      return false;
    }
  }

  public static boolean requireContainerToken(Type cmdType) {
    switch (cmdType) {
    case CloseContainer:
    case CreateContainer:
    case DeleteContainer:
    case ReadContainer:
    case UpdateContainer:
    case ListBlock:
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
    DatanodeBlockID blockID = null;
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
    case ReadBlock:
      if (msg.hasReadBlock()) {
        blockID = msg.getReadBlock().getBlockID();
      }
      break;
    case WriteChunk:
      if (msg.hasWriteChunk()) {
        blockID = msg.getWriteChunk().getBlockID();
      }
      break;
    case FinalizeBlock:
      if (msg.hasFinalizeBlock()) {
        blockID = msg.getFinalizeBlock().getBlockID();
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
    Objects.requireNonNull(path, "Path should not be null");
    Objects.requireNonNull(ancestor, "Ancestor should not be null");
    Preconditions.checkArgument(
        path.normalize().startsWith(ancestor.normalize()),
        "Path %s should be a descendant of %s", path, ancestor);
  }

  public static File createDir(String dirPath) {
    File dirFile = new File(dirPath);
    if (!dirFile.mkdirs() && !dirFile.exists()) {
      throw new IllegalArgumentException("Unable to create path: " + dirFile);
    }
    return dirFile;
  }

  /**
   * Utility string formatter method to display SCM roles.
   *
   * @param nodes
   * @return String
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

  /**
   * Return Ozone service shutdown time out.
   * @param conf
   */
  public static long getShutDownTimeOut(ConfigurationSource conf) {
    return conf.getObject(OzoneServiceConfig.class).getServiceShutdownTimeout();
  }

  /**
   * Utility method to round up bytes into the nearest MB.
   */
  public static int roundupMb(long bytes) {
    return (int)Math.ceil((double) bytes / (double) ONE_MB);
  }

  /**
   * Unwrap exception to check if it is some kind of access control problem
   * ({@link org.apache.hadoop.security.AccessControlException} or
   * {@link org.apache.hadoop.security.token.SecretManager.InvalidToken})
   * or a RpcException.
   */
  public static Throwable getUnwrappedException(Exception ex) {
    Throwable t = ex;
    if (ex instanceof ServiceException) {
      t = ex.getCause();
    }
    if (t instanceof RemoteException) {
      t = ((RemoteException) t).unwrapRemoteException();
    }
    if (t instanceof UndeclaredThrowableException) {
      t = ((UndeclaredThrowableException) t).getUndeclaredThrowable();
    }
    while (t != null) {
      if (t instanceof RpcException ||
          t instanceof AccessControlException ||
          t instanceof SecretManager.InvalidToken) {
        break;
      }
      Throwable cause = t.getCause();
      if (cause == null || cause instanceof RemoteException) {
        break;
      }
      t = cause;
    }
    return t;
  }

  /**
   * For some Rpc Exceptions, client should not failover.
   */
  public static boolean shouldNotFailoverOnRpcException(Throwable exception) {
    if (exception instanceof RpcException) {
      // Should not failover for following exceptions
      if (exception instanceof RpcNoSuchMethodException ||
          exception instanceof RpcNoSuchProtocolException ||
          exception instanceof RPC.VersionMismatch) {
        return true;
      }
      if (exception.getMessage().contains(
          "RPC response exceeds maximum data length") ||
          exception.getMessage().contains("RPC response has invalid length")) {
        return true;
      }
    }
    return exception instanceof InvalidProtocolBufferException;
  }

  /**
   * Remove binary data from request {@code msg}.  (May be incomplete, feel
   * free to add any missing cleanups.)
   */
  public static String processForDebug(ContainerCommandRequestProto msg) {
    if (msg == null) {
      return null;
    }

    if (msg.hasWriteChunk() || msg.hasPutBlock() || msg.hasPutSmallFile()) {
      final ContainerCommandRequestProto.Builder builder = msg.toBuilder();
      if (msg.hasWriteChunk()) {
        if (builder.getWriteChunkBuilder().hasData()) {
          builder.getWriteChunkBuilder()
              .setData(REDACTED);
        }

        if (builder.getWriteChunkBuilder().hasChunkData()) {
          builder.getWriteChunkBuilder()
              .getChunkDataBuilder()
              .clearChecksumData();
        }

        if (builder.getWriteChunkBuilder().hasBlock()) {
          builder.getWriteChunkBuilder()
              .getBlockBuilder()
              .getBlockDataBuilder()
              .getChunksBuilderList()
              .forEach(ContainerProtos.ChunkInfo.Builder::clearChecksumData);
        }
      }

      if (msg.hasPutBlock()) {
        builder.getPutBlockBuilder()
            .getBlockDataBuilder()
            .getChunksBuilderList()
            .forEach(ContainerProtos.ChunkInfo.Builder::clearChecksumData);
      }

      if (msg.hasPutSmallFile()) {
        builder.getPutSmallFileBuilder().setData(REDACTED);
      }
      return TextFormat.shortDebugString(builder);
    }

    return TextFormat.shortDebugString(msg);
  }

  /**
   * Remove binary data from response {@code msg}.  (May be incomplete, feel
   * free to add any missing cleanups.)
   */
  public static String processForDebug(ContainerCommandResponseProto msg) {

    if (msg == null) {
      return null;
    }

    if (msg.hasReadChunk() || msg.hasGetSmallFile()) {
      final ContainerCommandResponseProto.Builder builder = msg.toBuilder();
      if (msg.hasReadChunk()) {
        if (msg.getReadChunk().hasData()) {
          builder.getReadChunkBuilder().setData(REDACTED);
        }
        if (msg.getReadChunk().hasDataBuffers()) {
          builder.getReadChunkBuilder().getDataBuffersBuilder()
              .clearBuffers()
              .addBuffers(REDACTED);
        }
      }
      if (msg.hasGetSmallFile()) {
        if (msg.getGetSmallFile().getData().hasData()) {
          builder.getGetSmallFileBuilder().getDataBuilder().setData(REDACTED);
        }
        if (msg.getGetSmallFile().getData().hasDataBuffers()) {
          builder.getGetSmallFileBuilder().getDataBuilder()
              .getDataBuffersBuilder()
                  .clearBuffers()
                  .addBuffers(REDACTED);
        }
      }
      return TextFormat.shortDebugString(builder);
    }

    return TextFormat.shortDebugString(msg);
  }

  /**
   * Redacts sensitive configuration.
   * Sorts all properties by key name
   *
   * @param conf OzoneConfiguration object to be printed.
   * @return Sorted Map of properties
   */
  public static Map<String, String> processForLogging(OzoneConfiguration conf) {
    Map<String, String> ozoneProps = conf.getOzoneProperties();
    ConfigRedactor redactor = new ConfigRedactor(conf);
    Map<String, String> sortedOzoneProps = new TreeMap<>();
    for (Map.Entry<String, String> entry : ozoneProps.entrySet()) {
      String value = redactor.redact(entry.getKey(), entry.getValue());
      if (value != null) {
        value = value.trim();
      }
      sortedOzoneProps.put(entry.getKey(), value);
    }
    return sortedOzoneProps;
  }

  @Nonnull
  public static String threadNamePrefix(@Nullable Object id) {
    return id != null && !"".equals(id)
        ? id + "-"
        : "";
  }

  /**
   * Transform a protobuf UUID to Java UUID.
   */
  public static UUID fromProtobuf(HddsProtos.UUID uuid) {
    Objects.requireNonNull(uuid, "HddsProtos.UUID can't be null to transform to java UUID.");
    return new UUID(uuid.getMostSigBits(), uuid.getLeastSigBits());
  }

  /**
   * Transform a Java UUID to protobuf UUID.
   */
  public static HddsProtos.UUID toProtobuf(UUID uuid) {
    Objects.requireNonNull(uuid, "UUID can't be null to transform to protobuf UUID.");
    return HddsProtos.UUID.newBuilder()
        .setMostSigBits(uuid.getMostSignificantBits())
        .setLeastSigBits(uuid.getLeastSignificantBits())
        .build();
  }

  /** Concatenate stack trace {@code elements} (one per line) starting at
   * {@code startIndex}. */
  public static @Nonnull String formatStackTrace(
      @Nullable StackTraceElement[] elements, int startIndex) {
    if (elements != null && elements.length > startIndex) {
      final StringBuilder sb = new StringBuilder();
      for (int line = startIndex; line < elements.length; line++) {
        sb.append(elements[line]).append('\n');
      }
      return sb.toString();
    }
    return "";
  }

  /** @return current thread stack trace if {@code logger} has debug enabled */
  public static @Nullable StackTraceElement[] getStackTrace(
      @Nonnull Logger logger) {
    return logger.isDebugEnabled()
        ? Thread.currentThread().getStackTrace()
        : null;
  }

  /** @return Hex string representation of {@code value} */
  public static String checksumToString(long value) {
    return Long.toHexString(value);
  }

  /**
   * Logs a warning to report that the class is not closed properly.
   */
  public static void reportLeak(Class<?> clazz, String stackTrace, Logger log) {
    String warning = String.format("%s is not closed properly", clazz.getSimpleName());
    if (stackTrace != null && log.isDebugEnabled()) {
      String debugMessage = String.format("%nStackTrace for unclosed instance: %s",
          stackTrace);
      warning = warning.concat(debugMessage);
    }
    log.warn(warning);
  }

  /**
   * Get SCM ServiceId from OzoneConfiguration.
   * @return SCM service id if defined, else null.
   */
  public static String getScmServiceId(ConfigurationSource conf) {

    String localScmServiceId = conf.getTrimmed(
        ScmConfigKeys.OZONE_SCM_DEFAULT_SERVICE_ID);

    Collection<String> scmServiceIds;

    if (localScmServiceId == null) {
      // There is no default scm service id is being set, fall back to ozone
      // .scm.service.ids.
      scmServiceIds = conf.getTrimmedStringCollection(
          OZONE_SCM_SERVICE_IDS_KEY);
      if (scmServiceIds.size() > 1) {
        throw new ConfigurationException("When multiple SCM Service Ids are " +
            "configured," + OZONE_SCM_DEFAULT_SERVICE_ID + " need to be " +
            "defined");
      } else if (scmServiceIds.size() == 1) {
        localScmServiceId = scmServiceIds.iterator().next();
      }
    }
    return localScmServiceId;
  }

  /**
   * Get a collection of all scmNodeIds for the given scmServiceId.
   */
  public static Collection<String> getSCMNodeIds(ConfigurationSource conf,
                                                 String scmServiceId) {
    String key = ConfUtils.addSuffix(ScmConfigKeys.OZONE_SCM_NODES_KEY, scmServiceId);
    return conf.getTrimmedStringCollection(key);
  }

  /**
   * Get SCM Node Id list.
   * @param configuration
   * @return list of node ids.
   */
  public static Collection<String> getSCMNodeIds(
      ConfigurationSource configuration) {
    String scmServiceId = getScmServiceId(configuration);
    return getSCMNodeIds(configuration, scmServiceId);
  }
}
